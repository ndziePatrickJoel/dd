## c2s quotidien par stkb

from sqlalchemy import Column, String, Integer, Float, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import random


Base = declarative_base()


class DDC2SJourSTKB(Base):
    __tablename__ = 'dd_c2s_jour_stkb'
    id = Column(Integer, primary_key=True, name="id")
    jour =  Column(String(),  name="jour")
    stkbMsisdn = Column(String(), name="stkb_msisdn")
    category = Column(String(), name="category")
    userStatus = Column(String(), name="user_status")
    geographicalDomain = Column(String(), name="geographical_domain")
    stkpMsisdn = Column(String(), name="stkp_msisdn")
    stkpName = Column(String(), name="stkp_name")
    microzone = Column(String(), name="microzone")
    stkaName = Column(String(), name="stka_name")
    stkaMsisdn = Column(String(), name="stka_msisdn")
    partnerName = Column(String(), name="partner_name")
    partnerId = Column(String(), name="partner_id")
    stkaMsisdn = Column(String(), name="stka_msisdn")
    zonePMO = Column(String(), name="zone_pmo")
    regionCom = Column(String(), name="region_com")
    regionAdm = Column(String(), name="region_adm")
    acvi = Column(String(), name="acvi")
    c2s = Column(Float(), name="c2s")
    sem = Column(Float(), name="sem")
    

    def __init__(self, *args):
        """Si on passe à __init__ un ou plusieurs arguments, le premier doit être une ligne contenant la liste des informations valables
        pour un stkb"""
        if len(args) > 0:
            row = args[0]
            #print(row)
            self.stkbMsisdn = row[35]
            self.category = row[37]
            #self.userStatus = row[2]
            #self.geographicalDomain = row[3]
            self.stkpName = str(row[38])
            self.stkpMsisdn = str(row[39])
            #self.stkaName = row[38]
            #self.microzone = row[7]
            #self.acvi = row[8]
            #self.partnerName = row[9]
            self.stkaMsisdn = row[41]
            #self.zonePMO = row[11]
            #self.regionCom = row[12]
            #self.regionAdm = row[13]
            self.c2s = (float(row[45].replace(",", "")) / random.randint(1, 15)) * random.randint(1, 60)

    def __repr__(self):
            return "(id='%s', stkbMsisdn = '%s', stkpMsisdn = '%s') " % (self.id, self.stkbMsisdn, self.stkpMsisdn)

    def initFromRow(self, row):
        """Cette fonction permet d'initialiser un élément avec une ligne contenue dans le fichier brute,
        les numéros des colonnes doivent être respectés"""

        