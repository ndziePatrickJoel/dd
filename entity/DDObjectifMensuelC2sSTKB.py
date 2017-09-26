## c2s quotidien par stkb
from sqlalchemy import Column, String, Integer, Float, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from isoweek import Week
from datetime import datetime, date, timedelta
from entity.DDC2SJourSTKB import DDC2SJourSTKB
from db_utils.DBManager import SessionFactory



Base = declarative_base()


class DDObjectifMensuelC2sSTKB(Base):
    


    _DATE_FORMAT = "%d/%m/%Y"
    
    __tablename__ = 'dd_objectif_mensuel_c2s_stkb'
    id = Column(Integer, primary_key=True, name="id")
    mois =  Column(String(),  name="mois")
    stkbMsisdn = Column(String(), name="stkb_msisdn")
    stkpMsisdn = Column(String(), name="stkp_msisdn")
    stkaMsisdn = Column(String(), name="stka_msisdn")
    partnerName = Column(String(), name="partner_name")
    partnerId = Column(Integer(), name="partner_id")
    zonePMO = Column(String(), name="zone_pmo")
    regionCom = Column(String(), name="region_com")
    regionAdm = Column(String(), name="region_adm")
    acvi = Column(String(), name="acvi")
    objectif = Column(Float(), name="objectif")
    

    def __init__(self, row = None):
        """Si on passe à __init__ un ou plusieurs arguments, le premier doit être une ligne contenant la liste des informations valables
        pour un stkb"""

        if(row != None):
            self.stkbMsisdn =  str(row[0]).strip()
            self.stkpMsisdn = str(row[4]).strip()
            self.stkaMsisdn = str(row[8]).strip()
            self.acvi = str(row[9]).strip()
            self.zonePMO = str(row[11]).strip()
            self.regionCom = str(row[12]).strip()
            self.regionAdm = str(row[13]).strip()
            self.objectif = float(str(row[14]).strip().replace(" ", ""))                      
                  

    def __repr__(self):
            return "(id='%s', stkbMsisdn = '%s', stkpMsisdn = '%s', objectif = '%s')> " % (self.id, self.stkbMsisdn, self.stkpMsisdn, self.objectif)

    def initFromRow(self, row):
        """Cette fonction permet d'initialiser un élément avec une ligne contenue dans le fichier brute,
        les numéros des colonnes doivent être respectés"""

        pass


                  

    
    

                
                


