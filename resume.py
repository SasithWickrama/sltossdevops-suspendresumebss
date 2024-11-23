from datetime import datetime

import cx_Oracle

import const
import db

conn = db.DbConnection.dbconnBss("")
CDATE = datetime.now().strftime('%Y%m%d')

class Resume:
    def bssUpdate(self):

        if self['SERVICE_TYPE'] == 'V-VOICE COPPER':
            SV_TYPE = 'VOICE'
            ACCESS_TECH = 'COPPER'

        if self['SERVICE_TYPE'] == 'V-VOICE FTTH':
            SV_TYPE = 'VOICE'
            ACCESS_TECH = 'FTTH'

        if self['SERVICE_TYPE'] == 'BB-INTERNET COPPER':
            SV_TYPE = 'BB'
            ACCESS_TECH = 'COPPER'

        if self['SERVICE_TYPE'] == 'BB-INTERNET FTTH':
            SV_TYPE = 'BB'
            ACCESS_TECH = 'FTTH'

        if self['SERVICE_TYPE'] == 'E-IPTV FTTH':
            SV_TYPE = 'PEOTV'
            ACCESS_TECH = 'FTTH'

        if self['SERVICE_TYPE'] == 'E-IPTV COPPER':
            SV_TYPE = 'PEOTV'
            ACCESS_TECH = 'COPPER'


        UPDATE_VAL = 'ACTIVE';

        try:

            const.loggerresres.info(self[
                                  'LOGREF'] + "  " + "Start Suspend: =========================================================================")
            const.loggerresres.info(self['LOGREF'] + "  " + str(self))

            with conn.cursor() as cursor:
                result = cursor.var(cx_Oracle.STRING)
                # call the stored procedure
                cursor.callproc('CRCDIT.pkg_gnv_data_manipulation.c_product_attri_update_direct',
                                [self['TPNO'], ACCESS_TECH, self['SWITCH'], SV_TYPE, UPDATE_VAL, CDATE, '', result])

            const.loggerresres.info(self['LOGREF'] + "  " + str(result))
            const.loggerresres.info(self[
                                  'LOGREF'] + "  " + "End   : =========================================================================")
            print('Result '+str(result))
            return result.getvalue()


        except Exception as e:
            print(str(e))
            const.loggerresres.error(self['LOGREF'] + "  " + str(e))
            const.loggerresres.info(self[
                                  'LOGREF'] + "  " + "End   : =========================================================================")
            return str(e)
