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
    
    __tablename__ = 'dd_objectif_mensuel_c2s_zone_pmo'
    id = Column(Integer, primary_key=True, name="id")
    mois =  Column(String(),  name="mois")
    zonePMO = Column(String(), name="zone_pmo")
    regionCom = Column(String(), name="region_com")
    regionAdm = Column(String(), name="region_adm")
    acvi = Column(String(), name="acvi")
    objectif = Column(Float(), name="objectif")
    

    def __init__(self, row = None):
        """Si on passe à __init__ un ou plusieurs arguments, le premier doit être une ligne contenant la liste des informations valables
        pour un stkb"""

        if(row != None):    
            self.acvi = row['acvi']
            self.zonePMO = row['zone_pmo']
            self.regionCom = row['region_com']
            self.regionAdm = row['region_adm']
            self.mois = row['mois']
            self.objectif = row['objectif']                     
                  

    def __repr__(self):
            return "(id='%s', zonePMO = '%s', regionCom = '%s', objectif = '%s')> " % (self.id, self.zonePMO, self.regionCom, self.objectif)

    def initFromRow(self, row):
        """Cette fonction permet d'initialiser un élément avec une ligne contenue dans le fichier brute,
        les numéros des colonnes doivent être respectés"""

        pass


                  

    
    

                
                


