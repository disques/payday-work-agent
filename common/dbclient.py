import cx_Oracle

class Database:
    def __init__(self, conn_info):
        self.conn_str = conn_info.__str__()
        self.conn = cx_Oracle.connect(self.conn_str, encoding='utf-8')
        self.cursor = self.conn.cursor()

    def makeDictFactory(self, cursor):
        # columnNames = [d[0] for d in cursor.description]
        columnNames = []
        for d in cursor.description:
            try:
                columnNames.append(d[0])
            except Exception as e:
                print("colunm name error")
                print(e)

        def createRow(*args):
            return dict(zip(columnNames, args))
        return createRow

    def query(self, query):
        result = self.cursor.execute(query)
        self.cursor.rowfactory = self.makeDictFactory(self.cursor) 
        row = result.fetchall()
        return row

    def execute(self, query, data):
        result = self.cursor.execute(query, data)
        self.conn.commit()
        return result

    def executeNoCommit(self, query, data):
        result = self.cursor.execute(query, data)
        return result

    def commit(self):
        self.conn.commit()


    def close_all(self):
        self.cursor.close()
        self.conn.close()
                
        
class ConnectionInfo():
    def __init__(self, conn_dict):
        '''
        conn_dict = {
            'user':,
            'psw':,
            'host':,
            'port':,
            'service':,
        }
        '''
        self.user = conn_dict['user']
        self.psw = conn_dict['psw']
        self.host = conn_dict['host']
        self.port = conn_dict['port']
        self.service = conn_dict['service']
        self.conn_str = f'{self.user}/{self.psw}@{self.host}:{self.port}/{self.service}'

    def __str__(self):
        return self.conn_str
