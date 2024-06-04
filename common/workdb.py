import os
import time
import sys
import pymysql

class workdb:
    def __init__(self):
        self.conn = pymysql.connect(host='219.250.49.141', user='', password='', db='working', charset='utf8')
        self.curs = self.conn.cursor(pymysql.cursors.DictCursor)

    def setTime(self):
        self.curs.execute("select fron_unixtime()")
        return self.curs.fetchall()

    def insert_eip_record(self, data):
        sql = "insert into eip_record(sabun, yymmdd, go_time, leave_time, employee_nm, diff_in_time, yymm, absence_flag, basic_go, basic_leave, over_time) \
                values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        try:
            self.curs.executemany(sql, data)
            self.conn.commit()
        except Exception as e:
            print(e)

    def insert_caps_record(self, data):
        sql = "insert into caps_record(kpa_flag, str_date, g_id, e_date, e_time, e_idno, e_name, yymm) \
                values(%s, %s, %s, %s, %s, %s, %s, %s)"
        try:
            self.curs.executemany(sql, data)
            self.conn.commit()
        except Exception as e:
            print(e)

    def insert_eip_total_record(self, data):
        sql = "insert into total_record(yymm, sabun, total_time, leave_total_time, employee_nm, dept_nm, over_time) \
                values(%s, %s, %s, %s, %s, %s, %s)"
        try:
            self.curs.executemany(sql, data)
            self.conn.commit()
        except Exception as e:
            print(e)

    def insert_absence_record(self, data):
        sql = "insert into absence_record(cur_date, sabun, book_flag, note, employee_nm) \
                values(%s, %s ,%s, %s, %s)"
        try:
            self.curs.executemany(sql, data)
            self.conn.commit()
        except Exception as e:
            print(e)

    def insert_caps_io(self, data):
        sql = "insert into caps_io(sabun, yymmdd, iodata) \
                values(%s, %s ,%s)"
        try:
            self.curs.executemany(sql, data)
            self.conn.commit()
        except Exception as e:
            print(e)

    def insert_gec_record(self, data):
        sql = "insert into gec_record(sabun, yymmdd, gec_v001, gec_v006, eip_sabun) \
                values(%s, %s, %s, %s, %s)"
        try:
            self.curs.executemany(sql, data)
            self.conn.commit()
        except Exception as e:
            print(e)

    def insert_caps_daily_record(self, data):
        sql = "insert into caps_daily_record(yymmdd, sabun, sum_time) \
                values(%s, %s, %s)"
        try:
            self.curs.executemany(sql, data)
            self.conn.commit()
        except Exception as e:
            print(e)


    def delete_table(self, yymm):
        try:
            sql = "delete from absence_record where cur_date like '"+yymm+"%'"
            self.curs.execute(sql)

            sql = "delete from caps_daily_record where yymmdd like '"+yymm+"%'"
            self.curs.execute(sql)

            sql = "delete from caps_io where yymmdd like '"+yymm+"%'"
            self.curs.execute(sql)

            sql = "delete from eip_record where yymmdd like '"+yymm+"%'"
            self.curs.execute(sql)

            sql = "delete from gec_record where yymmdd like '"+yymm+"%'"
            self.curs.execute(sql)

            sql = "delete from caps_record where yymm='"+yymm+"'"
            self.curs.execute(sql)

            sql = "delete from total_record where yymm='"+yymm+"'"
            self.curs.execute(sql)

            self.conn.commit()

            print(yymm, "delete table done!")
        except Exception as e:
            print('delete table error', e)
