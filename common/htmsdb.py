import inspect
import common.settings as setting
from datetime import datetime
from common.dbclient import Database, ConnectionInfo

class htmsdb:
    def __init__(self, conn_info):
        self.db = Database(conn_info=ConnectionInfo(conn_info))


    def getGecData(self, yy, mm):
        sql = "select g.*, (select htmpersabunold from htmhrinsaper where htmcomcd='payday' and htmpersabun=g.gecpersabun) as eip_sabun \
                from gecdata g where geccomcd=rpad('payday', 12, ' ') \
                and (extract(month from gecd001) = "+mm+" or extract(month from gecd002) = "+mm+") \
                and (extract(year from gecd001) = "+yy+" or extract(year from gecd002) = "+yy+") "

        rows = self.db.query(query=sql)
        return rows
