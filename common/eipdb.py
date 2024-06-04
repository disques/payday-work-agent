import inspect
import common.settings as setting
from datetime import datetime
from common.dbclient import Database, ConnectionInfo

class eipdb:
    def __init__(self, conn_info):
        self.db = Database(conn_info=ConnectionInfo(conn_info))

    def getAttendanceBook(self, yy, mm):
        query = "select at.*, ep.employee_nm, de.dept_nm from GW_ATTENDANCE_BOOK at, GW_EMPLOYEE ep, GW_DEPT de where \
                    at.company_cd=ep.company_cd and at.sabun=ep.sabun and ep.company_cd=de.company_cd and ep.dept_cd=de.dept_cd and \
                    at.company_cd='HIMG' and at.yy='"+yy+"' and at.mm='"+mm+"' order by at.sabun, yy, mm, dd"
        rows = self.db.query(query=query)
        return rows
            
    def getAbsenceBook(self, yy, mm):
        query = "select ab.*, ep.employee_nm, ep.go_work_time, ep.leave_work_time, de.dept_nm from GW_ABSENCE_BOOK ab, GW_EMPLOYEE ep, GW_DEPT de where ab.company_cd='HIMG' \
                and ab.company_cd=de.company_cd and ep.dept_cd=de.dept_cd \
                and ab.company_cd=ep.company_cd and ab.sabun=ep.sabun \
                and (extract(month from ab.absence_from_date) = "+mm+" or extract(month from ab.absence_to_date) = "+mm+") \
                and (extract(year from ab.absence_from_date) = "+yy+" or extract(year from ab.absence_to_date) = "+yy+") "
                
        rows = self.db.query(query=query)
        return rows
