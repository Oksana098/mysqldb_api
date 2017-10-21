import _mysql


class MysqlDBApi(object):

    def __init__(self, host, user, passwd, database, port):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.database = database
        self.port = port

    def db_conector(self):
        return _mysql.connect(host=self.host, )


m = MysqlDBApi(host='127.0.0.1', user='root', passwd='', database='sf_tes', port='3306')

print

