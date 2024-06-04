import pyodbc

class capsdb:
    def __init__(self):
        server = '219.250.49.252'
        database = 'ACSDB' 
        username = '' 
        password = ''
        cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                      f'SERVER={server};'
                      f'DATABASE={database};'
                      f'UID={username};'
                      f'PWD={password};'
                      )
        self.cursor = cnxn.cursor()

    def query(self, query):
        results = []
        try:
            self.cursor.execute(query)
            columns = [column[0] for column in self.cursor.description]
            for row in self.cursor.fetchall():
                results.append(dict(zip(columns, row)))

        except Exception as e:
            print(e)

        return results
    
    def selectCaps(self, ym, sabun):
        select = "select e_date+e_time as strToDate, a.* \
                    from dbo.tenter a \
                    where e_date like '"+ym+"%' and e_idno='"+sabun+"' and g_id!=5 order by strToDate asc"
        return self.query(select)

#if __name__ == '__main__':
#    capsdb = Mssql()
#    rows = capsdb.query("select e_date+e_time as strToDate, a.* \
#                    from dbo.tenter a \
#                    where e_date = '20240223' \
#                    order by e_time")
#    for row in rows:
#        print(row)
