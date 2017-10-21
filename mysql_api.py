import _mysql


class MysqlDBApi(object):
    def __init__(self, host, user, passwd, database, port):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.database = database
        self.port = port
        self.con = self.db_connector()

    def db_connector(self):
        """
        Connector to database
        """
        return _mysql.connect(host=self.host, user=self.user, port=self.port, passwd=self.passwd, db=self.database)

    def create_insert_query(self, insert=None, **kwargs):
        """
        Create insert query into table
        :param insert: if set None generate from dict x=y, if select=True generate query for insert
        :param kwargs: dict with data
        :return: string
        """
        name_column = ''
        name_values = ''
        simple_equal = ''
        x = 0

        for key, val in kwargs.iteritems():
            x += 1
            if insert is True:
                if x != len(kwargs):
                    name_column += key + ', '
                else:
                    name_column += key

                if x != len(kwargs):
                    name_values += "'" + val + "'" + ', '
                else:
                    name_values += "'" + val + "'"

            else:
                if x != len(kwargs):
                    simple_equal += key + '=' + '"' + val + '", '
                else:
                    simple_equal += key + '=' + '"' + val + '"'

        if insert is True:
            return '(' + name_column + ') ' + 'values' + ' (' + name_values + ')'

        return simple_equal

    def insert(self, table, **kwargs):
        """
        Insert data into table
        :param table: Name of table in db
        :param kwargs: dict with data
        """
        return self.con.query("""INSERT {} {}""".format(table, self.create_insert_query(insert=True, **kwargs)))

    def select(self, select, store_result=None, use_result=None, fetch_row=None, maxrows=None):
        """
        Get data from table
        :param select: mysql query
        :param store_result: returns the entire result set to the client immediately
        :param use_result: keeps the result set in the server and sends it row-by-row when you fetch
        :param fetch_row: getting real results
        :param maxrows: how much return values from table, maxrows=0 return all entries
        :return:
        """
        self.con.query("""{}""".format(select))
        result = None

        if store_result is True:
            result = self.con.store_result()

        if use_result is True:
            result = self.con.use_result()

        if fetch_row is True and maxrows is not None:
            return result.fetch_row(maxrows=maxrows)
        else:
            return result.fetch_row()

    def update(self, table, set, where):
        """
        :param table: table in db
        :param set: element which update
        :param where: condition
        """
        set_new = self.create_insert_query(**set)
        where_new = self.create_insert_query(**where)
        return self.con.query("""UPDATE {}  SET {} WHERE {}""".format(table, set_new, where_new))

    def delete(self, table, where):
        """
        :param table: table in db
        :param where: condition
        """
        where_new = self.create_insert_query(**where)
        return self.con.query("""DELETE FROM {} WHERE {}""".format(table, where_new))


db = MysqlDBApi(host='127.0.0.1', user='root', passwd='', database='sf_tes', port=3306)

data = {'firstname': 'oksana', 'lastname': 'pidhurska'}

# 1. Insert
db.insert('users', **data)

# 2. Select
db.select('select * from users', use_result=True, fetch_row=True, maxrows=0)

# 3. Update
db.update(table='users', set={'firstname': "oleg"}, where={'firstname': "oleh2"})

# 4. Delete
db.delete(table='users', where={'firstname': "oksana"})
