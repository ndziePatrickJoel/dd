
from entity.DDC2CJourSTKA import DDC2CJourSTKA
import time
import calendar
from db_utils.DBManager import SessionFactory
from isoweek import Week
from datetime import date, timedelta
import datetime
import sys, traceback
from commons.Utils import Utils


class C2CSTKADataLoader:

    _DATE_FORMAT = '%d/%m/%Y'


    def insertC2CSTKADay(self, day):
        
        sessionFactory = SessionFactory()

        session = sessionFactory.Session()

        stkas = session.query(DDC2CJourSTKA).all()

        resultat = dict()

        for stka in stkas:
            resultat[stka] = stka
        

        for key, val in resultat.items:
            stkpStkaDay = session.query(DDC2CJourSTKP).filter_by(jour = day, stkp_msisdn = )
        
    

    
