## c2s quotidien par stkb
from sqlalchemy import Column, String, Integer, Float, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from isoweek import Week
from datetime import datetime, date, timedelta
from entity.DDC2SJourSTKB import DDC2SJourSTKB
from db_utils.DBManager import SessionFactory



Base = declarative_base()


class DDObjectifMensuelC2sSTKA(Base):
    


    _DATE_FORMAT = "%d/%m/%Y"
    
    __tablename__ = 'dd_objectif_mensuel_c2s_stka'
    id = Column(Integer, primary_key=True, name="id")
    mois =  Column(String(),  name="mois")
    stkaMsisdn = Column(String(), name="stka_msisdn")
    partnerName = Column(String(), name="partner_name")
    partnerId = Column(Integer(), name="partner_id")
    regionCom = Column(String(), name="region_com")
    regionAdm = Column(String(), name="region_adm")
    acvi = Column(String(), name="acvi")
    objectif = Column(Float(), name="objectif")
    

    def __init__(self, row = None):
        """Si on passe à __init__ un ou plusieurs arguments, le premier doit être une ligne contenant la liste des informations valables
        pour un stkb"""

        if(row != None):
            self.stkaMsisdn = row['stka_msisdn']
            self.acvi = row['acvi']
            self.regionCom = row['region_com']
            self.regionAdm = row['region_adm']
            self.objectif =  row['objectif']
            self.mois = row['mois']                      
                  

    def __repr__(self):
            return "(id='%s', stkaMsisdn = '%s', regioncom = '%s', objectif = '%s')> " % (self.id, self.stkaMsisdn, self.regionCom, self.objectif)

    def initFromRow(self, row):
        """Cette fonction permet d'initialiser un élément avec une ligne contenue dans le fichier brute,
        les numéros des colonnes doivent être respectés"""

        pass


                  

    
    

                
                


