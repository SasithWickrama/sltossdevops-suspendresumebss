import multiprocessing
import random
import sys
from datetime import datetime
from  resume import Resume
from suspend import Suspend
import db

conn = db.DbConnection.dbconnPrg("")
cmonth = datetime.now().strftime('%Y%m')
data = {}


def specific_string(length):
    sample_string = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890'  # define the specific string
    # define the condition for random string
    return ''.join((random.choice(sample_string)) for x in range(length))


def suspend(typ, x):
    if typ == 'VOICE':
        sql = 'select ROWID,LEA,SERVICE_ID,REQ_BY,SERVICE_TYPE,ORDER_TYPE,CR,ACCNO ,CCT,STATUS ' \
              'from  EXPROV_VOICE_' + cmonth + ' where STATUS IN(100) AND ORDER_TYPE IN (\'MODI-PARTIAL SUSPEND\',\'SUSPEND\')'  \
               'AND MOD(DBMS_ROWID.ROWID_ROW_NUMBER(EXPROV_VOICE_' + cmonth + '.ROWID), 5) = ' + str( x)

    if typ == 'BB':
        sql = 'select ROWID,LEA,SERVICE_ID,REQ_BY,SERVICE_TYPE,ORDER_TYPE,CR,ACCNO ,CCT,STATUS ' \
              'from  EXPROV_BB_' + cmonth + ' where STATUS IN(100) AND ORDER_TYPE IN (\'MODI-PARTIAL SUSPEND\',\'SUSPEND\')' \
              'AND MOD(DBMS_ROWID.ROWID_ROW_NUMBER(EXPROV_BB_' + cmonth + '.ROWID), 5) = ' + str( x)

    if typ == 'IPTV':
        sql = 'select ROWID,LEA,SERVICE_ID,REQ_BY,SERVICE_TYPE,ORDER_TYPE,CR,ACCNO ,CCT,STATUS ' \
              'from  EXPROV_IPTV_' + cmonth + ' where STATUS IN(100) AND ORDER_TYPE IN (\'MODI-PARTIAL SUSPEND\',\'SUSPEND\')' \
              'AND MOD(DBMS_ROWID.ROWID_ROW_NUMBER(EXPROV_IPTV_' + cmonth + '.ROWID), 5) = ' + str( x)

    c = conn.cursor()
    c.execute(sql)

    for row in c:
        ROWID,LEA,SERVICE_ID, REQ_BY, SERVICE_TYPE, ORDER_TYPE, CR, ACCNO, CCT, STATUS = row

        sql2 = 'select distinct SATT_DEFAULTVALUE from clarity.services_attributes where ' \
            'SATT_SERV_ID=:SATT_SERV_ID and SATT_ATTRIBUTE_NAME = :SATT_ATTRIBUTE_NAME'
        c2 = conn.cursor()
        c2.execute(sql2, [SERVICE_ID, 'MANS_NAME'])
        row2 = c2.fetchone()

        data['ROWID'] = ROWID
        data['LEA'] = LEA
        data['SERVICE_ID'] = SERVICE_ID
        data['REQ_BY'] = REQ_BY
        data['SERVICE_TYPE'] = SERVICE_TYPE
        data['ORDER_TYPE'] = ORDER_TYPE
        data['CR'] = CR
        data['ACCNO'] = ACCNO
        data['TPNO'] = CCT
        data['STATUS'] = STATUS

        if row2[0] == 'IMS':
            data['SWITCH'] = 'IMS'
        elif row2[0] == 'ZTE':
            data['SWITCH'] = 'SZ'
        elif row2[0] == 'HUAWEI':
            data['SWITCH'] = 'SH'
        else:
            data['SWITCH'] = 'NA'

        refid = specific_string(15)
        data['LOGREF'] = refid

        print(data)

        result = Suspend.bssUpdate(data)

        if result == 'SUCCESS':
            if typ == 'VOICE':
                sqlupdate = 'update EXPROV_VOICE_' + cmonth + ' set STATUS=:STATUS, STATUS_DATE=sysdate,BSS_STAT=:BSS_STAT where  ROWID= :ROW_ID and STATUS=:STAT'
                with conn.cursor() as cursor2:
                    cursor2.execute(sqlupdate, [150, result, ROWID, STATUS])
                    conn.commit()
                    print(cursor2.rowcount)
            if typ == 'BB':
                sqlupdate = 'update EXPROV_BB_' + cmonth + ' set STATUS=:STATUS, STATUS_DATE=sysdate,BSS_STAT=:BSS_STAT where  ROWID= :ROW_ID and STATUS=:STAT'
                with conn.cursor() as cursor2:
                    cursor2.execute(sqlupdate, [150, result, ROWID, STATUS])
                    conn.commit()
                    print(cursor2.rowcount)
            if typ == 'IPTV':
                sqlupdate = 'update EXPROV_IPTV_' + cmonth + ' set STATUS=:STATUS, STATUS_DATE=sysdate,BSS_STAT=:BSS_STAT where  ROWID= :ROW_ID and STATUS=:STAT'
                with conn.cursor() as cursor2:
                    cursor2.execute(sqlupdate, [150, result, ROWID, STATUS])
                    conn.commit()
                    print(cursor2.rowcount)
        else:
            if typ == 'VOICE':
                sqlupdate = 'update EXPROV_VOICE_' + cmonth + ' set STATUS=:STATUS, STATUS_DATE=sysdate,BSS_STAT=:BSS_STAT where  ROWID= :ROW_ID and STATUS=:STAT'
                with conn.cursor() as cursor2:
                    cursor2.execute(sqlupdate, [150, result, ROWID, STATUS])
                    conn.commit()
                    print(cursor2.rowcount)

            if typ == 'BB':
                sqlupdate = 'update EXPROV_BB_' + cmonth + ' set STATUS=:STATUS, STATUS_DATE=sysdate,BSS_STAT=:BSS_STAT where  ROWID= :ROW_ID and STATUS=:STAT'
                with conn.cursor() as cursor2:
                    cursor2.execute(sqlupdate, [150, result, ROWID, STATUS])
                    conn.commit()
                    print(cursor2.rowcount)

            if typ == 'IPTV':
                sqlupdate = 'update EXPROV_IPTV_' + cmonth + ' set STATUS=:STATUS, STATUS_DATE=sysdate,BSS_STAT=:BSS_STAT where  ROWID= :ROW_ID and STATUS=:STAT'
                with conn.cursor() as cursor2:
                    cursor2.execute(sqlupdate, [150, result, ROWID, STATUS])
                    conn.commit()
                    print(cursor2.rowcount)


def resume(typ, x):
    if typ == 'VOICE':
        sql = 'select ROWID,LEA,SERVICE_ID,REQ_BY,SERVICE_TYPE,ORDER_TYPE,CR,ACCNO ,CCT,STATUS ' \
              'from  EXPROV_VOICE_' + cmonth + ' where STATUS IN(100) AND ORDER_TYPE IN (\'MODI-PARTIAL RESUME\',\'RESUME\')' \
               'AND MOD(DBMS_ROWID.ROWID_ROW_NUMBER(EXPROV_VOICE_' + cmonth + '.ROWID), 5) = ' + str(x)
    if typ == 'BB':
        sql = 'select ROWID,LEA,SERVICE_ID,REQ_BY,SERVICE_TYPE,ORDER_TYPE,CR,ACCNO ,CCT,STATUS ' \
              'from  EXPROV_BB_' + cmonth + ' where STATUS IN(100) AND ORDER_TYPE IN (\'MODI-PARTIAL RESUME\',\'RESUME\')' \
              'AND MOD(DBMS_ROWID.ROWID_ROW_NUMBER(EXPROV_BB_' + cmonth + '.ROWID), 5) = ' + str(x)
    if typ == 'IPTV':
        sql = 'select ROWID,LEA,SERVICE_ID,REQ_BY,SERVICE_TYPE,ORDER_TYPE,CR,ACCNO ,CCT,STATUS ' \
              'from  EXPROV_IPTV_' + cmonth + ' where STATUS IN(100) AND ORDER_TYPE IN (\'MODI-PARTIAL RESUME\',\'RESUME\')' \
              'AND MOD(DBMS_ROWID.ROWID_ROW_NUMBER(EXPROV_IPTV_' + cmonth + '.ROWID), 5) = ' + str(x)

    c = conn.cursor()
    c.execute(sql)

    for row in c:
        ROWID,LEA,SERVICE_ID, REQ_BY, SERVICE_TYPE, ORDER_TYPE, CR, ACCNO, CCT, STATUS = row

        sql2 = 'select distinct SATT_DEFAULTVALUE from clarity.services_attributes where ' \
               'SATT_SERV_ID=:SATT_SERV_ID and SATT_ATTRIBUTE_NAME = :SATT_ATTRIBUTE_NAME'
        c2 = conn.cursor()
        c2.execute(sql2, [SERVICE_ID, 'MANS_NAME'])
        row2 = c2.fetchone()

        data['ROWID'] = ROWID
        data['LEA'] = LEA
        data['SERVICE_ID'] = SERVICE_ID
        data['REQ_BY'] = REQ_BY
        data['SERVICE_TYPE'] = SERVICE_TYPE
        data['ORDER_TYPE'] = ORDER_TYPE
        data['CR'] = CR
        data['ACCNO'] = ACCNO
        data['TPNO'] = CCT
        data['STATUS'] = STATUS

        if row2[0] == 'IMS':
            data['SWITCH'] = 'IMS'
        elif row2[0] == 'ZTE':
            data['SWITCH'] = 'SZ'
        elif row2[0] == 'HUAWEI':
            data['SWITCH'] = 'SH'
        else:
            data['SWITCH'] = 'NA'

        refid = specific_string(15)
        data['LOGREF'] = refid

        print(data)

        result = Resume.bssUpdate(data)

        if result == 'SUCCESS':
            if typ == 'VOICE':
                sqlupdate = 'update EXPROV_VOICE_' + cmonth + ' set STATUS=:STATUS, STATUS_DATE=sysdate,BSS_STAT=:BSS_STAT where  ROWID= :ROW_ID and STATUS=:STAT'
                with conn.cursor() as cursor2:
                    cursor2.execute(sqlupdate, [150, result, ROWID, STATUS])
                    conn.commit()
                    print(cursor2.rowcount)
            if typ == 'BB':
                sqlupdate = 'update EXPROV_BB_' + cmonth + ' set STATUS=:STATUS, STATUS_DATE=sysdate,BSS_STAT=:BSS_STAT where  ROWID= :ROW_ID and STATUS=:STAT'
                with conn.cursor() as cursor2:
                    cursor2.execute(sqlupdate, [150, result, ROWID, STATUS])
                    conn.commit()
                    print(cursor2.rowcount)
            if typ == 'IPTV':
                sqlupdate = 'update EXPROV_IPTV_' + cmonth + ' set STATUS=:STATUS, STATUS_DATE=sysdate,BSS_STAT=:BSS_STAT where  ROWID= :ROW_ID and STATUS=:STAT'
                with conn.cursor() as cursor2:
                    cursor2.execute(sqlupdate, [150, result, ROWID, STATUS])
                    conn.commit()
                    print(cursor2.rowcount)
        else:
            if typ == 'VOICE':
                sqlupdate = 'update EXPROV_VOICE_' + cmonth + ' set STATUS=:STATUS, STATUS_DATE=sysdate,BSS_STAT=:BSS_STAT where  ROWID= :ROW_ID and STATUS=:STAT'
                with conn.cursor() as cursor2:
                    cursor2.execute(sqlupdate, [150, result, ROWID, STATUS])
                    conn.commit()
                    print(cursor2.rowcount)
            if typ == 'BB':
                sqlupdate = 'update EXPROV_BB_' + cmonth + ' set STATUS=:STATUS, STATUS_DATE=sysdate,BSS_STAT=:BSS_STAT where  ROWID= :ROW_ID and STATUS=:STAT'
                with conn.cursor() as cursor2:
                    cursor2.execute(sqlupdate, [150, result, ROWID, STATUS])
                    conn.commit()
                    print(cursor2.rowcount)
            if typ == 'IPTV':
                sqlupdate = 'update EXPROV_IPTV_' + cmonth + ' set STATUS=:STATUS, STATUS_DATE=sysdate,BSS_STAT=:BSS_STAT where  ROWID= :ROW_ID and STATUS=:STAT'
                with conn.cursor() as cursor2:
                    cursor2.execute(sqlupdate, [150, result, ROWID, STATUS])
                    conn.commit()
                    print(cursor2.rowcount)


if __name__ == '__main__':
    processes = []

    if sys.argv[1] == 'SUSPEND':
        for i in range(0, 5):
            if sys.argv[2] == 'VOICE':
                p = multiprocessing.Process(target=suspend, args=('VOICE', i,))

            if sys.argv[2] == 'BB':
                p = multiprocessing.Process(target=suspend, args=('BB', i,))

            if sys.argv[2] == 'IPTV':
                p = multiprocessing.Process(target=suspend, args=('IPTV', i,))

            processes.append(p)
            p.start()

    if sys.argv[1] == 'RESUME':
        for i in range(0, 5):
            if sys.argv[2] == 'VOICE':
                p = multiprocessing.Process(target=resume, args=('VOICE', i,))

            if sys.argv[2] == 'BB':
                p = multiprocessing.Process(target=resume, args=('BB', i,))

            if sys.argv[2] == 'IPTV':
                p = multiprocessing.Process(target=resume, args=('IPTV', i,))

            processes.append(p)
            p.start()

    # multiprocessing_func(i)
    for process in processes:
        process.join()
