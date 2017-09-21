## c2s quotidien par stkb

from sqlalchemy import Column, String, Integer, Float, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import random


Base = declarative_base()


class DDC2CJourSTKP(Base):
    __tablename__ = 'dd_c2c_jour_stkp'
    id = Column(Integer, primary_key=True, name="id")
    jour =  Column(String(),  name="jour")
    quartierCom = Column(String(), name="quartier_com")
    route = Column(String(), name="route")
    stkpMsisdn = Column(String(), name="stkp_msisdn")
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
    

    def __init__(self, *args):
        """Si on passe à __init__ un ou plusieurs arguments, le premier doit être une ligne contenant la liste des informations valables
        pour un stkb"""
        if len(args) > 0:
            row = args[0]
            #print(row)
            self.stkpMsisdn = row[44]
            #self.category = row[37]
            #self.userStatus = row[2]
            #self.geographicalDomain = row[3]
            self.route = str(row[43])
            self.stkaName = str(row[45])
            #self.microzone = row[7]
            #self.acvi = row[8]
            #self.partnerName = row[9]
            self.stkaMsisdn = row[46]
            #self.zonePMO = row[11]
            #self.regionCom = row[12]
            #self.regionAdm = row[13]
            self.c2c = (float(row[49].replace(",", "")) / random.randint(1, 15)) * random.randint(1, 60)

    def __repr__(self):
            return "(id='%s', stkpMsisdn = '%s', stkaMsisdn = '%s', c2c = '%s', stkaName = '%s') " % (self.id, self.stkpMsisdn, self.stkaMsisdn, self.c2c, self.stkaName)

    def initFromRow(self, row):
        """Cette fonction permet d'initialiser un élément avec une ligne contenue dans le fichier brute,
        les numéros des colonnes doivent être respectés"""

        