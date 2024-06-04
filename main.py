import shutil
import os
import time
import multiprocessing
import logging
import re
import subprocess
import sys

from datetime import datetime, timedelta

import common.settings
import common.settings as setting

from common.eipdb import eipdb
from common.capsdb import capsdb
from common.workdb import workdb
from common.htmsdb import htmsdb

def noneCheck(value):
    return value if value != None else ''

def get_all_dates(from_date, to_date):
    start_date = datetime.strptime(from_date, "%Y%m%d")
    end_date = datetime.strptime(to_date, "%Y%m%d")

    # Generate a list of all dates between the start and end dates
    all_dates = []
    current_date = start_date
    while current_date <= end_date:
        all_dates.append(current_date.strftime("%Y%m%d"))
        current_date += timedelta(days=1)

    return all_dates

def time_difference(go, leave):
    format_str = "%Y%m%d%H%M" # 날짜와 시간 형식 지정
    datetime1 = datetime.strptime(go, format_str)
    datetime2 = datetime.strptime(leave, format_str)

    time_difference = datetime2 - datetime1
    difference_in_minutes = time_difference.total_seconds() / 60

    return int(difference_in_minutes)

def get_caps_data(capsDB, key):
    sp = key.split('-')
    rows = capsDB.selectCaps(sp[1], sp[0])

    caps_dict = {}
    for row in rows:
        e_date = row['e_date']
        e_idno = row['e_idno']
        key = e_idno+"-"+e_date

        if key in caps_dict:
            caps_dict[key].append(row)
        else:
            caps_dict[key] = [row]
    return caps_dict

def gid(gid):
    if gid == 1 or gid == 3:
        return 'I'
    elif gid == 2 or gid == 4:
        return 'O'
    elif gid == 6:
        return 'X'

def caps_io_calculator(outs, sp):
    sum_time = 0
    for i in range(0, len(outs), 2):

        pair = outs[i:i+2]
        if pair[0]["D"]["g_id"] != 2 and pair[0]["D"]["g_id"] != 4:
            if pair[0]["K"] != "X" and pair[0]["K"] != "K":
                print(sp[0], sp[1], pair[0]["K"], "나가는 기록 부터 시작이 아닌 경우 제외")
            continue

        if pair[1]["D"]["g_id"] != 1 and pair[1]["D"]["g_id"] != 3:
            if pair[1]["K"] != "X" and pair[1]["K"] != "K":
                print(sp[0], sp[1], pair[1]["K"], "들어오는 데이터가 아니라서 제외 함")
            continue

        if int(pair[0]["D"]["e_time"]) < 60000:
            print(sp[0], "새벽 6시 이전 데이터라서 일단 제외")
            continue

        diff_time = 0
        out_time = sp[1]+pair[0]["D"]["e_time"]
        in_time = sp[1]+pair[1]["D"]["e_time"]

        o = int(pair[0]["D"]["e_time"])
        i = int(pair[1]["D"]["e_time"])
        # 점심시간 이전에 나가서 점심시간에 들어온 경우
        # ex 12:00 -> 12:30 (12:00 -> 12:15)
        if o<121500 and i>121500 and i<131500:
            # print("점심시간 이전에 나가서 점심시간에 들어온 경우")
            in_time = sp[1]+"121500"
            diff_time = time_difference(out_time[:-2], in_time[:-2])

        # 점심시간 이전에 나가서 점심시간 이후에 들어온 경우
        # 12:00 -> 13:20 (12:00 -> 12:15, 13:15 -> 13:20)
        elif o<121500 and i>131500:
            # print("점심시간 이전에 나가서 점심시간 이후에 들어온 경우", end= ' ')
            diff1 = time_difference(out_time[:-2], sp[1]+"1215")
            diff2 = time_difference(sp[1]+"1315", in_time[:-2])

            diff_time = diff1+diff2

         # 점심시간 중에 나가서 점심 시간 이후에 들어온 경우
         # 12:20 -> 13:20 (13:15 -> 13:20)
        elif o>121500 and o<131500 and i>131500:
            # print("점심시간 중에 나가서 점심시간 이후에 들어온 경우")
            out_time = sp[1]+"131500"
            diff_time = time_difference(out_time[:-2], in_time[:-2])

        # 점심 시간 중에 나가서 점심시간 중에 들어온 경우
        elif o>121500 and o<131500 and i<131500:
            o
            # print(sp, "점심  시간 중에 나가서 점심시간 중에 들어온 경우")

        # 그외 모든 경우
        else:
            diff_time = time_difference(out_time[:-2], in_time[:-2])

        if pair[1]["D"]["kpa_flag"] == "T":
            diff_time = 0

        if diff_time < 0:
            print("diff_time 이 마이너스??")
            diff_time = 0

        sum_time = sum_time + diff_time

    return sum_time

def caps_data_conversion(caps_dict, workDB):
    results = []
    output_data = []
    for k, vs in caps_dict.items():
        sp = k.split("-")
        sum_time = 0
        output_array = []
        next_flag = 1
        kpa_flag = "F"
        caps_data = []

        for v in vs:
            g_id = v['g_id']
            v['kpa_flag'] = kpa_flag
            v['kpa'] = 'X'
            if next_flag == 1:
                if g_id == 1 or g_id == 3:
                    output_array.append({"K": "I", "D": v})
                    next_flag = 2
                    kpa_flag = "F"
                elif g_id == 6:
                    v['kpa'] = 'O'
                    output_array.append({"K": "K", "D": v})
                    output_array.append({"K": "K", "D": v})
                    kpa_flag = "T"
                else:
                    output_array.append({"K": "X", "D": v})
                    output_array.append({"K": "O", "D": v})
                    kpa_flag = "F"
            else:
                if g_id == 2 or g_id == 4:
                    output_array.append({"K": "O", "D": v})
                    next_flag = 1
                    kpa_flag = "F"
                elif g_id == 6:
                    v['kpa'] = 'O'
                    output_array.append({"K": "K", "D": v})
                    output_array.append({"K": "K", "D": v})
                    kpa_flag = "T"
                else:
                    output_array.append({"K": "X", "D": v})
                    output_array.append({"K": "I", "D": v})
                    kpa_flag = "F"
            caps_data.append(
                (v['kpa_flag'], v['strToDate'], v['g_id'], v['e_date'], v['e_time'], v['e_idno'], v['e_name'], v['e_date'][:-2])
            )
        workDB.insert_caps_record(caps_data)

        io_data = ""
        sepr = ""
        for d in output_array:
            e_time = d["D"]["e_time"]
            flag = d["K"]
            if flag == "X":
                e_time = "000000"
            io_data = io_data+sepr+str(e_time)+flag
            sepr = "-"
        output_data.append(
            (sp[0], sp[1], io_data)
        )

        output_len = len(output_array)
        if output_array[0]["K"] != "X":
            # 들어와서 나가는 기록이 정확한 경우에만 시간을 계산
            if output_array[0]["K"] == "I" and output_array[output_len-1]["K"] == "O":
                outs = output_array[1:-1]
                sum_time = sum_time + caps_io_calculator(outs, sp)
            else:
                if output_array[0]["K"] != "I":
                    # X 로 짝을 맞추기 때문에 이 로그가 보이는 경우는 확인 해야 할 듯
                    print("처음 들어온 기록이 없는 경우", end=' ')
                    print(output_array[0]["K"], output_array[output_len-1]["K"], k, len(vs))
                elif output_array[output_len-1]["K"] != "O":
                    # print("마지막 나간 기록이 없는 경우", end= ' ')
                    # print(output_array[0]["K"], output_array[output_len-1]["K"], k, len(vs))
                    outs = output_array[1:]
                    outs_len = len(outs)
                    # if outs[outs_len-1]["K"] != "I": 
                    if outs_len % 2 == 0:
                        sum_time = sum_time + caps_io_calculator(outs, sp)
                    else:
                        print(sp[0], "pair  수가 안맞음!")
                    # else:
                    #    print(sp[0], "마지막에 하나를 제거 했는데도 퇴근 기록이 없음")
        else:
            # 이경우도 별도로 기록해서 표기 - 하루가 지나서 기록 되는 경우인듯
            # print("첫 기록 부터 짝이 안맞는 경우", end=' ')
            # print(output_array[0]["K"], output_array[output_len-1]["K"], k, len(vs))
            if len(vs) > 4:
                output_len = len(output_array)
                if output_array[output_len-1]["K"] != "I":
                    outs = output_array[3:-1]
                    outs_len = len(outs)
                    # print(sp[0], "시작 기록이 잘못된 경우, 마지막 기록이 나간경우", outs_len)
                    if outs_len % 2 == 0:
                        sum_time = sum_time + caps_io_calculator(outs, sp)
                    else:
                        print(sp[0], "pair  수가 안맞음!", outs_len)
                else:
                    print(sp[0], "첫 기록이 짝이 안맞아서 첫기록 빼고 했는데 마지막 퇴근 기록도 없음", len(output_array))
            # else:
                # print("첫 기록 짝이 안맞는데 건수도 4건이라서 의미가 없음")

        results.append({"D": k, "S": sum_time})

    workDB.insert_caps_io(output_data)
    return results

def setData(sabun, date, time, employee_nm, yymm, absence_flag, dept_nm, go_work_time, leave_work_time):
    return {
        "sabun": sabun,
        "yymmdd": date,
        "diff_in_time": time,
        "go_time": "0", 
        "leave_time": "0",
        "employee_nm": employee_nm,
        "yymm": yymm,
        "absence_flag": absence_flag,
        "dept_nm": dept_nm,
        "basic_go": go_work_time,
        "basic_leave": leave_work_time,
        "over_time": "0"
    }

def absence_conversion(data_dict, absences, workDB, year, month):
    absence_data = []
    for absence in absences:
        sabun = absence['SABUN']
        to_date = absence['ABSENCE_TO_DATE'].strftime('%Y%m%d')
        from_date = absence['ABSENCE_FROM_DATE'].strftime('%Y%m%d')
        try:
            datas = data_dict[sabun+'-'+year+month]
        except KeyError:
            print(f"key error '{sabun}-{year}{month}' 데이터가 없습니다.")
            continue
        go_work_time = absence['GO_WORK_TIME']
        leave_work_time = absence['LEAVE_WORK_TIME']

        # 휴가 기간은 근무 한것으로 처리
        if absence['ABSENCE_BOOK_FLAG'].strip() != '3':
            all_dates = get_all_dates(from_date, to_date)
            for cur_date in all_dates:
                absence_data.append(
                    (cur_date, absence["SABUN"], absence["ABSENCE_BOOK_FLAG"], absence["ABSENCE_NOTE"], absence["EMPLOYEE_NM"])
                )
                isFind = False
                for data in datas:
                    if data['yymmdd'] == cur_date:
                        isFind = True
                        break
                if isFind == False:
                    data = setData(sabun, cur_date, 480, absence['EMPLOYEE_NM'], cur_date[:-2], "O", absence['DEPT_NM'], go_work_time, leave_work_time)
                    data_dict[sabun+'-'+year+month].append(data)

    workDB.insert_absence_record(absence_data)
def worker(year, month):
    while True:
        if len(sys.argv) < 3:
            now = datetime.now()
            year = now.strftime("%Y")
            month = now.strftime("%m")
            print(f"year and month not provided, using default values: {year} {month}")
        else:
            year = str(sys.argv[1])
            month = str(sys.argv[2])
            
        print("year", year, "month", month)

        data_dict = {}
        absence_dict = {}
        try:
            info = common.settings.eip
            eipDB = eipdb(conn_info=info)
            capsDB = capsdb()
            workDB = workdb()
            workDB.delete_table(str(year)+str(month))

            paydayDB = htmsdb(conn_info=common.settings.payday)

            absences = eipDB.getAbsenceBook(year, month)
            rows = eipDB.getAttendanceBook(year, month)

            gecs = paydayDB.getGecData(year, month)
            # print(gecs)
            gec_data = []
            for gec in gecs:
                if gec["EIP_SABUN"] is None:
                    gec["EIP_SABUN"] = gec["GECPERSABUN"].strip()

                from_date = gec["GECD001"].strftime("%Y%m%d")
                to_date = gec["GECD002"].strftime("%Y%m%d")
                all_dates = get_all_dates(from_date, to_date)
                for cur_date in all_dates:
                    gec_data.append(
                        (gec["GECPERSABUN"].strip(), cur_date, gec["GECV001"], "1" if len(all_dates) > 1 else gec["GECV006"], gec["EIP_SABUN"])
                    )
            workDB.insert_gec_record(gec_data)

            # 부재 등록 정보 가져오기 # 여기서 근태 기록이 없는 부재등록은 기록을 임의로 넣어줘야하나???
            # 아니면 계산할 때 해야하나??
            for absence in absences:
                abs_key = absence['SABUN']
                if abs_key in absence_dict:
                    absence_dict[abs_key].append(absence)
                else:
                    absence_dict[abs_key] = [absence]

            for row in rows:
                try:
                    sabun = row['SABUN']
                    yy = row['YY'].strip()
                    mm = row['MM'].strip()
                    dd = row['DD'].strip()
                
                    # 부서명
                    dept_nm = row["DEPT_NM"].strip()

                    basic_go_work_time = row['BASIC_GO_WORK_TIME'].strip()
                    basic_leave_work_time = row['BASIC_LEAVE_WORK_TIME'].strip()

                except Exception as e:
                    print(sabun, yy, mm, dd, e)
                    continue

                employee_nm = row['EMPLOYEE_NM']

                # 퇴근 시간이 없는 경우에는 일단 제외
                if row['GO_WORK_TIME'] == None or row['LEAVE_WORK_TIME'] == None:
                    # print(row)
                    continue

                go_work_time = row['GO_WORK_TIME'].strip()
                leave_work_time = row['LEAVE_WORK_TIME'].strip()
                absence_flag = "X"

                # 부재 등록 있는지 확인 있으면 퇴근시간과 베이직 시간중 높은거 적용
                try:
                    if sabun in absence_dict:
                        abs_array = absence_dict[sabun]
                        for abse in abs_array:
                            if abse['ABSENCE_BOOK_FLAG'].strip() != '3':
                                ymd = yy+"-"+mm+"-"+dd
                                to_date = abse['ABSENCE_TO_DATE'].strftime('%Y-%m-%d')
                                from_date = abse['ABSENCE_FROM_DATE'].strftime('%Y-%m-%d')
                                if ymd == to_date or ymd == from_date:
                                    # print(sabun, ymd, to_date, from_date)
                                    # go 시간은 작은 시간, leave 시간은 늦은 시간을 적용 
                                    if int(go_work_time) > int(basic_go_work_time):
                                        go_work_time = basic_go_work_time
                                    if int(leave_work_time) < int(basic_leave_work_time):
                                        leave_work_time = basic_leave_work_time
                                    absence_flag = "O"
                        
                except Exception as e:
                    print('0', e)

                go = yy+mm+dd+go_work_time
                lunch_go = yy+mm+dd+'1215'
                leave = yy+mm+dd+leave_work_time;
                lunch_leave = yy+mm+dd+'1315'

                diff1 = 0
                diff2 = 0
                over_time = 0

                # 퇴근 시간이 0000 을 넘어서는 경우 예외처리
                if leave_work_time > "0000" and leave_work_time < "0730":
                    # print(sabun, "밤 12시를 넘어 퇴근한것으로 간주", "하루가 지나서 다음날 출근 시간을 넘는 경우는 고려 안함")

                    # datetime 객체로 변환
                    date_format = "%Y%m%d%H%M"
                    date_obj = datetime.strptime(leave, date_format)

                    # 하루 증가
                    one_day = timedelta(days=1)
                    new_date_obj = date_obj + one_day
                    
                    # 새로운 날짜를 문자열로 변환
                    leave = new_date_obj.strftime(date_format)


                if go > lunch_go and go < lunch_leave:
                    # print("점심 시간에 출근, 점심끝 - 퇴근")
                    diff1 = time_difference(lunch_leave, leave)
                    if diff1 < 0:
                        # print(sabun, "점심 시간중에 들어와서 점심 중에 나간 경우")
                        diff1 = 0

                elif go > lunch_leave:
                    # print("점심 시간 이후 출근, 출근 - 퇴근")
                    diff1 = time_difference(go, leave)
                else:
                    if leave < lunch_go:
                        # print("점심 이전 퇴근, 출근 - 퇴근")
                        diff1 = time_difference(go, leave)
                    elif leave > lunch_go and leave < lunch_leave:
                        # print("점심 중 퇴근, 출근 - 점심시작")
                        diff1 = time_difference(go, lunch_go) # 출근시간 - 점심시작
                    else:
                        # print("일반출근, 오전 오후 구분")
                        diff1 = time_difference(go, lunch_go)
                        diff2 = time_difference(lunch_leave, leave)

                # 저녁시간 계산 하는 로직 들어가야 할 듯
                # 하루가 지난 경우에 대한 로직이 들어가야 할 듯 (leave_work_time > 0000 값 확인)
                if leave_work_time > basic_leave_work_time:
                    basic_leave = yy+mm+dd+basic_leave_work_time
                    over_time = time_difference(basic_leave, leave)
                    # print(sabun, "over_time -> ", basic_leave, leave_work_time, over_time)
                    if over_time < 0:
                        print(sabun, "마이너스 값이 나옴", leave_work_time, basic_leave_work_time)

                key = sabun+"-"+yy+mm
                data = {
                        "sabun": sabun,
                        "yymmdd": yy+mm+dd,
                        "diff_in_time": diff1+diff2,
                        "go_time": go,
                        "leave_time": leave,
                        "employee_nm": employee_nm,
                        "yymm": yy+mm,
                        "absence_flag": absence_flag,
                        "dept_nm": dept_nm,
                        "basic_go": basic_go_work_time,
                        "basic_leave": basic_leave_work_time,
                        "over_time": over_time
                }

                if key in data_dict:
                    data_dict[key].append(data)
                else:
                    data_dict[key] = [data]

                # print(sabun, yy, mm, dd, difference_in_minutes)

            # 부재등록만 있고, 출퇴근 데이터가 없는 경우 임의로 만들어 줌
            absence_conversion(data_dict, absences, workDB, year, month)

            eip_total_data = []
            for key, values in data_dict.items():
                sp = key.split("-")
                total = 0
                emp_nm = "-"
                dept_nm = "-"

                eip_data = []
                total_over_time = 0
                for value in values: 
                    eip_data.append(
                            (
                                value["sabun"], value["yymmdd"], value["go_time"], value["leave_time"], 
                                value["employee_nm"], value["diff_in_time"], value["yymm"], value["absence_flag"],
                                value["basic_go"], value["basic_leave"], value["over_time"]
                            )
                                    )
                    total = total + int(value['diff_in_time'])
                    total_over_time = total_over_time + int(value['over_time'])

                    emp_nm = value['employee_nm']
                    dept_nm = value['dept_nm']

                workDB.insert_eip_record(eip_data)

                caps_dict = get_caps_data(capsDB, key)
                results = caps_data_conversion(caps_dict, workDB)

                leave_total_time = 0
                caps_daily_data = []
                for v in results:
                    _sp = v["D"].split("-")
                    caps_daily_data.append(
                        (_sp[1], _sp[0], v["S"])
                    )
                    leave_total_time = leave_total_time + int(v["S"])

                workDB.insert_caps_daily_record(caps_daily_data)
                eip_total_data.append(
                    (sp[1], sp[0], total, leave_total_time, emp_nm, dept_nm, total_over_time)
                )

                # print(f"Empnm: {emp_nm}, Key: {key}, Total: {total}, leave_total: {leave_total_time}")

            workDB.insert_eip_total_record(eip_total_data)
        except Exception as e:
            print('error', e)

        break


if __name__ == '__main__':
    worker(0, 0)
