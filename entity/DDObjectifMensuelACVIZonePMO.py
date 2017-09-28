## c2s quotidien par stkb
from sqlalchemy import Column, String, Integer, Float, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from isoweek import Week
from datetime import datetime, date, timedelta



Base = declarative_base()


class DDObjectifMensuelACVIZonePMO(Base):
    


    _DATE_FORMAT = "%d/%m/%Y"
    
    __tablename__ = 'dd_objectif_mensuel_acvi_zone_pmo'
    id = Column(Integer, primary_key=True, name="id")
    mois =  Column(String(),  name="mois")
    zonePMO = Column(String(), name="zone_pmo")
    regionCom = Column(String(), name="region_com")
    regionAdm = Column(String(), name="region_adm")
    objectif = Column(Float(), name="objectif")
    acvi = Column(String(), name="acvi")
    

    def __init__(self, row = None):
        """Si on passe à __init__ un ou plusieurs arguments, le premier doit être une ligne contenant la liste des informations valables
        pour un stkb"""

        if(row != None):    
            self.zonePMO = row['zone_pmo']
            self.regionCom = row['region_com']
            self.regionAdm = row['region_adm']
            self.mois = row['mois']
            self.objectif = row['objectif'] 
            self.acvi = row['acvi']
                  

    def __repr__(self):
            return "(id='%s', zonePMO = '%s', mois = '%s', objectif = '%s')> " % (self.id, self.zonePMO, self.mois, self.objectif)

    def initFromRow(self, row):
        """Cette fonction permet d'initialiser un élément avec une ligne contenue dans le fichier brute,
        les numéros des colonnes doivent être respectés"""

        pass


                  

    
    

                
                


