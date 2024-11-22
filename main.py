from datetime import datetime

import db

conn = db.DbConnection.dbconnHadwh("")
connerp = db.DbConnection.dbconnErp("")
connhrm = db.DbConnection.dbconnHrm("")
cmonth= datetime.now().strftime('%Y%m')

sql = 'select ROWID,RECORDED_TIME,MOBILE_NO,case when USERID is null then ' \
      '(select distinct EMP_NUMBER from SLT_EMP_TP where substr(MOBILE_NO,-9)=substr(TPNO,-9)) ' \
      'else USERID end as USERID, USER_ACTIVITY, IN_SYSTEM from ATTENDANCE_'+cmonth+' where DSTATUS is null '
c = conn.cursor()
c.execute(sql)

for row in c:
    ROW_ID,RECORDED_TIME, MOBILE_NO, USERID, USER_ACTIVITY, IN_SYSTEM = row
    print(ROW_ID,RECORDED_TIME, MOBILE_NO, USERID, USER_ACTIVITY, IN_SYSTEM)
    print(cmonth)

    matches = ["IN", "OUT"]

    if any(x in USER_ACTIVITY for x in matches):
        
        sqlerp = "INSERT INTO XXERP.HR_MOB_ATTENDANCE VALUES ( :RECORDED_TIME,:MOBILE_NO,:USERID,:USER_ACTIVITY,:IN_SYSTEM,:DSTATUS)"
        with connerp.cursor() as cursor:
            cursor.execute(sqlerp, [RECORDED_TIME, MOBILE_NO, USERID, USER_ACTIVITY, IN_SYSTEM, '0'])
            connerp.commit()
        sqlhadwh = "update ATTENDANCE_"+cmonth+" set DSTATUS=:DSTATUS where  ROWID= :ROW_ID and DSTATUS is null"
        with conn.cursor() as cursor2:
            cursor2.execute(sqlhadwh, ["10", ROW_ID])
            conn.commit()
        if USERID:
            sqlhrm = "INSERT INTO REMOTE_ACCESS_LOG (LOG_DATE,SERVICE_NUMBER,IN_OUT_STATUS,TERMINAL_ID,READ,H_ISUPDATE) VALUES (:LOG_DATE,:SERVICE_NUMBER,:IN_OUT_STATUS,:TERMINAL_ID,:READ,:H_ISUPDATE)"
            with connhrm.cursor() as cursor3:
                cursor3.execute(sqlhrm, [RECORDED_TIME, USERID,USER_ACTIVITY,IN_SYSTEM,1,1])
                connhrm.commit()
        print('success')
    else:
        print('failed')
        sqlhadwh = "update ATTENDANCE_"+cmonth+" set DSTATUS=:DSTATUS where  ROWID= :ROW_ID and DSTATUS is null"
        with conn.cursor() as cursor2:
            cursor2.execute(sqlhadwh, ["20", ROW_ID])
            conn.commit()

