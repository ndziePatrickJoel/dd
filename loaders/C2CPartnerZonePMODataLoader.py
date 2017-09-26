from entity.DDC2CJourSTKA import DDC2CJourSTKA
from entity.DDC2CJourSTKP import DDC2CJourSTKP
from entity.DDC2CSemSTKA import DDC2CSemSTKA
from entity.DDC2CMoisSTKA import DDC2CMoisSTKA
from entity.DDC2CJourPartnerZonePMO import DDC2CJourPartnerZonePMO
from entity.DDC2CWeekPartnerZonePMO import DDC2CWeekPartnerZonePMO
from entity.DDC2CMoisPartnerZonePMO import DDC2CMoisPartnerZonePMO
from entity.STKA import STKA
import time
import calendar
from db_utils.DBManager import SessionFactory
from isoweek import Week
from datetime import date, timedelta
import datetime
import sys, traceback
from commons.Utils import Utils



class C2CPartnerZonePMODataLoader:

    _DATE_FORMAT = '%d/%m/%Y'


    def insertC2CJourPartnerZonePMO(self, day):
        sessionFactory = SessionFactory()

        session = sessionFactory.Session()

        data = []
                        
        connection = sessionFactory.getConnection()
        result = connection.execute("select partner_id, partner_name, zone_pmo, region_adm, region_com, sem, jour, sum(c2c) c2c  from dd_c2c_jour_stkp where jour = '"+day+"' group by partner_id, zone_pmo")

        for row in result:
            tmpElt = DDC2CJourPartnerZonePMO(row)      
            data.append(tmpElt)

        connection.close()    

        try:
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Début insertion ")
            session.add_all(data)
            session.commit()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion ")
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertions avec succès")
        except Exception as ex:
            self.view_traceback()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Echec de l'insertion, annulation des modifications")
            session.rollback()
        finally:
            session.close()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Session close")

    def insertC2CWeekPartnerZonePMO(self, weekString):
        

        prevWeekString = Utils.getPrevWeekString(weekString)

        prevWeekData = self.getWeekValues(prevWeekString)

        weekData = self.getWeekValues(weekString)

        for c2c in weekData:
            for prevC2C in prevWeekData:
                if(prevC2C.partnerId == c2c.partnerId and prevC2C.zonePMO == c2c.zonePMO):
                    c2c.c2cS1 = prevC2C.c2c
                    c2c.evolS1 = c2c.c2c - prevC2C.c2c

        
        
        
        sessionFactory = SessionFactory()

        session = sessionFactory.Session()

        try:
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Début insertion ")
            session.add_all(weekData)
            session.commit()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion ")
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertions avec succès")
        except Exception as ex:
            self.view_traceback()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Echec de l'insertion, annulation des modifications")
            session.rollback()
        finally:
            session.close()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Session close")  


    def insertC2CMoisPartnerZonePMO(self, monthString):
        

        prevMonthString = Utils.getPrevMonthString(monthString)

        prevMonthData = self.getMonthValues(prevMonthString)

        monthData = self.getMonthValues(monthString)

        for c2c in monthData:
            for prevC2C in prevMonthData:
                if(prevC2C.partnerId == c2c.partnerId and prevC2C.zonePMO == c2c.zonePMO):
                    c2c.c2cM1 = prevC2C.c2c
                    c2c.evolM1 = c2c.c2c - prevC2C.c2c       
                
        sessionFactory = SessionFactory()
        session = sessionFactory.Session()

        try:
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Début insertion ")
            session.add_all(monthData)
            session.commit()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion ")
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertions avec succès")
        except Exception as ex:
            self.view_traceback()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Echec de l'insertion, annulation des modifications")
            session.rollback()
        finally:
            session.close()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Session close")    
  

    
    def getWeekValues(self, weekString):
        sessionFactory = SessionFactory()

        session = sessionFactory.Session()

        data = []
                        
        connection = sessionFactory.getConnection()
        result = connection.execute("select partner_id, partner_name, zone_pmo, region_adm, region_com, sem, sum(c2c) c2c  from dd_c2c_jour_stkp where sem = '"+weekString+"' group by partner_id, zone_pmo")

        for row in result:
            tmpElt = DDC2CWeekPartnerZonePMO(row)      
            data.append(tmpElt)

        connection.close()  

        return data

    def getMonthValues(self, month):
        sessionFactory = SessionFactory()

        session = sessionFactory.Session()

        data = []

        #01082017
                        
        connection = sessionFactory.getConnection()
        requete = "select partner_id, partner_name, zone_pmo, region_adm, region_com, concat(substr(replace(jour, '/', ''), 5), substr(replace(jour, '/', ''), 3, 2))  mois, sum(c2c) c2c  from dd_c2c_jour_stkp where concat(substr(replace(jour, '/', ''), 5), substr(replace(jour, '/', ''), 3, 2)) = '"+month+"' group by partner_id, zone_pmo"
        result = connection.execute(requete);

        for row in result:
            tmpElt = DDC2CMoisPartnerZonePMO(row)      
            data.append(tmpElt)

        connection.close()  

        return data

    def view_traceback(self):
        ex_type, ex, tb = sys.exc_info()
        traceback.print_tb(tb)
        del tb
        