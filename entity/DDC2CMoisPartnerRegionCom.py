## c2s quotidien par stkb
from sqlalchemy import Column, String, Integer, Float, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from isoweek import Week
from datetime import datetime, date, timedelta
from entity.DDC2SJourSTKB import DDC2SJourSTKB
from db_utils.DBManager import SessionFactory



Base = declarative_base()


class DDC2CMoisPartnerRegionCom(Base):
    
   

    _DATE_FORMAT = "%d/%m/%Y"
    
    __tablename__ = 'dd_c2c_mois_partner_region_com'
    id = Column(Integer, primary_key=True, name="id")
    mois =  Column(String(),  name="mois")
    partnerId = Column(Integer(), name="partner_id")
    partnerName = Column(String(), name="partner_name")
    c2c = Column(Float(), name="c2c")
    objectifC2c = Column(Float(), name="objectif_c2c")
    regionCom = Column(String(), name="region_com")
    c2cM1 = Column(Float(), name="c2c_m_1")
    evolM1 = Column(Float(), name="evol_m_1")


    def __init__(self, row = None):
        """Si on passe à __init__ un ou plusieurs arguments, le premier doit être une ligne contenant la liste des informations valables
        pour un stkb"""

        if row != None:
            self.mois = row['mois']
            self.partnerId = row['partner_id']
            self.partnerName = row['partner_name']
            self.regionCom = row['region_com']
            self.c2c = row['c2c']

                
    def __repr__(self):
            return "(id='%s', stkpMsisdn = '%s')> " % (self.id, self.partnerName)

    def initFromRow(self, row):
        """Cette fonction permet d'initialiser un élément avec une ligne contenue dans le fichier brute,
        les numéros des colonnes doivent être respectés"""

        pass


                  

    
    

                
                


