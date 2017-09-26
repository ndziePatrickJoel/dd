## c2s quotidien par stkb

from sqlalchemy import Column, String, Integer, Float, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import random


Base = declarative_base()


class DDC2CJourSTKA(Base):
    __tablename__ = 'dd_c2c_jour_stka'
    id = Column(Integer, primary_key=True, name="id")
    jour =  Column(String(),  name="jour")
    quartierCom = Column(String(), name="quartier_com")
    stkaName = Column(String(), name="stka_name")
    stkaMsisdn = Column(String(), name="stka_msisdn")
    partnerName = Column(String(), name="partner_name")
    partnerId = Column(String(), name="partner_id")
    zonePMO = Column(String(), name="zone_pmo")
    regionCom = Column(String(), name="region_com")
    regionAdm = Column(String(), name="region_adm")
    acvi = Column(String(), name="acvi")
    c2c = Column(Float(), name="c2c")
    sem = Column(Float(), name="sem")
    objectifC2c = Column(Float(), name="objectif_c2c")
    

    def __init__(self, stkp = None):
        """Si on passe à __init__ un ou plusieurs arguments, le premier doit être une ligne contenant la liste des informations valables
        pour un stkb"""   

        if stkp != None:
            self.stkaName = stkp.stkaName
            self.partnerName = stkp.partnerName
            self.partnerId = stkp.partnerId
            self.zonePMO = stkp.zonePMO
            self.regionCom = stkp.regionCom
            self.regionAdm = stkp.regionAdm
            self.acvi = stkp.acvi
            self.c2c = 0    

    def __repr__(self):
            return "(id='%s',  stkaMsisdn = '%s', c2c = '%s', stkaName = '%s') " % (self.id, self.stkaMsisdn, self.c2c, self.stkaName)

    def initFromRow(self, row):
        """Cette fonction permet d'initialiser un élément avec une ligne contenue dans le fichier brute,
        les numéros des colonnes doivent être respectés"""

        