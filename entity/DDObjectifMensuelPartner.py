## c2s quotidien par stkb
from sqlalchemy import Column, String, Integer, Float, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from isoweek import Week
from datetime import datetime, date, timedelta



Base = declarative_base()


class DDObjectifMensuelPartner(Base):
    


    _DATE_FORMAT = "%d/%m/%Y"
    
    __tablename__ = 'dd_objectif_mensuel_partner'
    id = Column(Integer, primary_key=True, name="id")
    partnerId = Column(Integer(), name="partner_id")
    partnerName = Column(String(), name="partner_name")
    mois =  Column(String(),  name="mois")
    objectif = Column(Float(), name="objectif")
    

    def __init__(self, row = None):
        """Si on passe à __init__ un ou plusieurs arguments, le premier doit être une ligne contenant la liste des informations valables
        pour un stkb"""

        if(row != None):    
            self.mois = row['mois']
            self.objectif = row['objectif'] 
            self.partnerId = row['partner_id']
            self.partnerName = row['partner_name']
                                
                  

    def __repr__(self):
            return "(id='%s', partnerName = '%s', mois = '%s', objectif = '%s')> " % (self.id, self.partnerName, self.mois, self.objectif)

    def initFromRow(self, row):
        """Cette fonction permet d'initialiser un élément avec une ligne contenue dans le fichier brute,
        les numéros des colonnes doivent être respectés"""

        pass


                  

    
    

                
                


