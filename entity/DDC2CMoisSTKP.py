## c2s quotidien par stkb

from sqlalchemy import Column, String, Integer, Float, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class DDC2CMoisSTKP(Base):
    __tablename__ = 'dd_c2s_mois_stkb'
    id = Column(Integer, primary_key=True, name="id")
    mois =  Column(String(),  name="sem")
    stkpMsisdn = Column(String(), name="stkp_msisdn")
    quartierCom = Column(String(), name="quartier_com")
    stkaName = Column(String(), name="stka_name")
    stkaMsisdn = Column(String(), name="stka_msisdn")
    partnerName = Column(String(), name="partner_name")
    partnerId = Column(Integer(), name="partner_id")
    zonePMO = Column(String(), name="zone_pmo")
    regionCom = Column(String(), name="region_com")
    regionAdm = Column(String(), name="region_adm")
    acvi = Column(String(), name="acvi")
    c2c = Column(Float(), name="c2c")
    c2cMoins1 = Column(Float(), name="c2c_m_1")
    evolutionM1 = Column(Float(), name="evol_m_1")
    objectifC2c = Column(Float(), name="objectif_c2c")

    



    def __init__(self, stkpDay = None):
        """Si on passe à __init__ un ou plusieurs arguments, le premier doit être une ligne contenant la liste des informations valables
        pour un stkb"""
        if  stkpDay != None:
            self.quartierCom = stkpDay.quartierCom
            self.route = stkpDay.route
            self.stkpMsisdn = stkpDay.stkpMsisdn
            self.stkaName = stkpDay.stkaName
            self.stkaMsisdn = stkpDay.stkaMsisdn
            self.partnerName = stkpDay.partnerName
            self.partnerId = stkpDay.partnerId
            self.zonePMO = stkpDay.zonePMO
            self.regionCom = stkpDay.regionCom
            self.regionAdm = stkpDay.regionAdm
            self.acvi = stkpDay.acvi            
            

    def __repr__(self):
            return "(id='%s', stkbMsisdn = '%s')> " % (self.id, self.stkbMsisdn)

    def initFromRow(self, row):
        """Cette fonction permet d'initialiser un élément avec une ligne contenue dans le fichier brute,
        les numéros des colonnes doivent être respectés"""

        