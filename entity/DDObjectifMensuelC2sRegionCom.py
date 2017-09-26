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
    
    __tablename__ = 'dd_objectif_mensuel_c2s_region_com'
    id = Column(Integer, primary_key=True, name="id")
    mois =  Column(String(),  name="mois")
    regionCom = Column(String(), name="region_com")
    objectif = Column(Float(), name="objectif")
    

    def __init__(self, row = None):
        """Si on passe à __init__ un ou plusieurs arguments, le premier doit être une ligne contenant la liste des informations valables
        pour un stkb"""

        if(row != None):
            self.regionCom = row['region_com']
            self.objectif = row['objectif']
            self.mois = row['mois']                  
                  
    def __repr__(self):
            return "(id='%s', regionCom = '%s', objectif = '%s', mois = '%s')> " % (self.id, self.regionCom, self.objectif, self.mois)

    def initFromRow(self, row):
        """Cette fonction permet d'initialiser un élément avec une ligne contenue dans le fichier brute,
        les numéros des colonnes doivent être respectés"""

        pass


                  

    
    

                
                


