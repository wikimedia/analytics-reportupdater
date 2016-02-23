
class ConnectionCursorMock(object):

    def __init__(self, execute_callback, fetchall_callback, header):
        self.execute_callback = execute_callback
        self.fetchall_callback = fetchall_callback
        self.description = [[header_field] for header_field in header]

    def execute(self, sql_query):
        if self.execute_callback:
            self.execute_callback(sql_query)

    def fetchall(self):
        if self.fetchall_callback:
            return self.fetchall_callback()

    def close(self):
        pass


class ConnectionMock(object):

    def __init__(self, execute_callback, fetchall_callback, header):
        self.execute_callback = execute_callback
        self.fetchall_callback = fetchall_callback
        self.header = header

    def cursor(self):
        return ConnectionCursorMock(self.execute_callback, self.fetchall_callback, self.header)
