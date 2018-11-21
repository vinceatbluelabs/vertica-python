from __future__ import print_function, division, absolute_import

import logging
import re
from collections import OrderedDict
from io import IOBase

import six
# noinspection PyUnresolvedReferences,PyCompatibility
from builtins import str
from six import binary_type, text_type, string_types, BytesIO, StringIO

try:
    from psycopg2.extensions import QuotedString
except ImportError:
    class QuotedString(object):
        def __init__(self, s):
            raise ImportError("couldn't import psycopg2.extensions.QuotedString")

from .. import errors
from ..compat import as_text
from ..vertica import messages
from ..vertica.column import Column

logger = logging.getLogger('vertica')

UTF_8 = 'utf-8'

if six.PY2:
    # noinspection PyUnresolvedReferences
    file_type = (IOBase, file)
elif six.PY3:
    file_type = (IOBase,)

# VMB Removing everything after this and replacing with an empty class fixed it

class Cursor(object):
    def __init__(self, connection, cursor_type=None, unicode_error='strict'):
        self.connection = connection
        self.cursor_type = cursor_type
        self.unicode_error = unicode_error
        self._closed = False
        self._message = None

        self.error = None

        #
        # dbapi properties
        #
        self.description = None
        self.rowcount = -1
        self.arraysize = 1

    #############################################
    # supporting `with` statements
    #############################################
    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.close()

    #############################################
    # dbapi methods
    #############################################
    def callproc(self, procname, parameters=None):
        raise errors.NotSupportedError('Cursor.callproc() is not implemented')

    def close(self):
        self._closed = True

    def execute(self, operation, parameters=None):
        operation = as_text(operation)

        if self.closed():
            raise errors.Error('Cursor is closed')

        self.flush_to_query_ready()

        if parameters:
            # TODO: quote = True for backward compatibility. see if should be False.
            operation = self.format_operation_with_parameters(operation, parameters, quote=True)

        self.rowcount = -1

        self.connection.write(messages.Query(operation))

        # read messages until we hit an Error, DataRow or ReadyForQuery
        self._message = self.connection.read_message()
        while True:
            if isinstance(self._message, messages.ErrorResponse):
                raise errors.QueryError.from_error_response(self._message, operation)
            elif isinstance(self._message, messages.RowDescription):
                self.description = [Column(fd, self.unicode_error) for fd in self._message.fields]
            elif isinstance(self._message, messages.DataRow):
                break
            elif isinstance(self._message, messages.ReadyForQuery):
                break
            elif isinstance(self._message, messages.CommandComplete):
                break
            else:
                self.connection.process_message(self._message)

            self._message = self.connection.read_message()

    def is_stringy(self, s):
        try:
            # python 2 case
            return isinstance(s, basestring)
        except NameError:
            # python 3 case
            return isinstance(s, str)

    def fetchone(self):
        if isinstance(self._message, messages.DataRow):
            if self.rowcount == -1:
                self.rowcount = 1
            else:
                self.rowcount += 1

            row = self.row_formatter(self._message)
            # fetch next message
            self._message = self.connection.read_message()
            return row
        elif isinstance(self._message, messages.ReadyForQuery):
            return None
        elif isinstance(self._message, messages.CommandComplete):
            return None
        else:
            self.connection.process_message(self._message)

    def iterate(self):
        row = self.fetchone()
        while row:
            yield row
            row = self.fetchone()

    def fetchmany(self, size=None):
        if not size:
            size = self.arraysize
        results = []
        while True:
            row = self.fetchone()
            if not row:
                break
            results.append(row)
            if len(results) >= size:
                break
        return results

    def fetchall(self):
        return list(self.iterate())

    def nextset(self):
        # skip any data for this set if exists
        self.flush_to_command_complete()

        if self._message is None:
            return None
        elif isinstance(self._message, messages.CommandComplete):
            # there might be another set, read next message to find out
            self._message = self.connection.read_message()
            if isinstance(self._message, messages.RowDescription):
                # next row will be either a DataRow or CommandComplete
                self._message = self.connection.read_message()
                return True
            elif isinstance(self._message, messages.ReadyForQuery):
                return None
            else:
                raise errors.Error('Unexpected nextset() state after CommandComplete: ' + str(self._message))
        elif isinstance(self._message, messages.ReadyForQuery):
            # no more sets left to be read
            return None
        else:
            raise errors.Error('Unexpected nextset() state: ' + str(self._message))


    def setinputsizes(self):
        pass

    def setoutputsize(self, size, column=None):
        pass

    #
    # Non dbApi methods
    #
    def flush_to_query_ready(self):
        # if the last message isnt empty or ReadyForQuery, read all remaining messages
        if(self._message is None
           or isinstance(self._message, messages.ReadyForQuery)):
            return

        while True:
            message = self.connection.read_message()
            if isinstance(message, messages.ReadyForQuery):
                self.connection.transaction_status = message.transaction_status
                self._message = message
                break

    def flush_to_command_complete(self):
        # if the last message isnt empty or CommandComplete, read messages until it is
        if(self._message is None
           or isinstance(self._message, messages.ReadyForQuery)
           or isinstance(self._message, messages.CommandComplete)):
            return

        while True:
            message = self.connection.read_message()
            if isinstance(message, messages.CommandComplete):
                self._message = message
                break


    # example:
    #
    # with open("/tmp/file.csv", "rb") as fs:
    #   cursor.copy("COPY table(field1,field2) FROM STDIN DELIMITER ',' ENCLOSED BY '\"'", fs, buffer_size=65536)
    #

    def copy(self, sql, data, **kwargs):

        if self.closed():
            raise errors.Error('Cursor is closed')

        self.flush_to_query_ready()

        self.connection.write(messages.Query(sql))

        while True:
            message = self.connection.read_message()

            if isinstance(message, messages.ErrorResponse):
                raise errors.QueryError.from_error_response(message, sql)

            self.connection.process_message(message=message)
            if isinstance(message, messages.ReadyForQuery):
                break
            elif isinstance(message, messages.CopyInResponse):

                #write stuff
                if not hasattr(data, "read"):
                    self.connection.write(messages.CopyData(data))
                else:
                    # treat data as stream
                    self.connection.write(messages.CopyStream(data, **kwargs))

                self.connection.write(messages.CopyDone())

        if self.error is not None:
            raise self.error

    #
    # Internal
    #

    def closed(self):
        return self._closed or self.connection.closed()

    def row_formatter(self, row_data):
        if self.cursor_type is None:
            return self.format_row_as_array(row_data)
        elif self.cursor_type in (list, 'list'):
            return self.format_row_as_array(row_data)
        elif self.cursor_type in (dict, 'dict'):
            return self.format_row_as_dict(row_data)
        else:
            raise Exception('Unrecognized cursor_type: %r' % self.cursor_type)

    def format_row_as_dict(self, row_data):
        return OrderedDict(
            (self.description[idx].name, self.description[idx].convert(value))
            for idx, value in enumerate(row_data.values)
        )

    def format_row_as_array(self, row_data):
        return [self.description[idx].convert(value)
                for idx, value in enumerate(row_data.values)]
