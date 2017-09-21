## c2s quotidien par stkb

from sqlalchemy import Column, String, Integer, Float, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class DDC2SAnneeSTKB(Base):
    __tablename__ = 'dd_c2s_annee_stkb'
    id = Column(Integer, primary_key=True, name="id")
    annee =  Column(String(),  name="mois")
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
    partnerId = Column(Integer(), name="partner_id")
    zonePMO = Column(String(), name="zone_pmo")
    regionCom = Column(String(), name="region_com")
    regionAdm = Column(String(), name="region_adm")
    acvi = Column(String(), name="acvi")
    c2s = Column(Float(), name="c2s")
    pointsProd = Column(Float(), name="points_prod")
    pointsAssiduite = Column(Float(), name="point_assiduite")
    pointsEvol = Column(Float(), "points_evol")
    totalPoints = Column(Float(), "total_points")
    



    def __init__(self, c2sWeekSTKB = None):
        """Si on passe à __init__ un ou plusieurs arguments, le premier doit être une ligne contenant la liste des informations valables
        pour un stkb"""
        if  c2sWeekSTKB != None:
            self.stkbMsisdn = c2sWeekSTKB.stkbMsisdn
            self.category = c2sWeekSTKB.category
            self.userStatus = c2sWeekSTKB.userStatus
            self.geographicalDomain = c2sWeekSTKB.geographicalDomain
            self.stkpName = c2sWeekSTKB.stkpName
            self.stkpMsisdn = c2sWeekSTKB.stkpMsisdn
            self.stkaName = c2sWeekSTKB.stkaName
            self.microzone = c2sWeekSTKB.microzone
            self.acvi = c2sWeekSTKB.acvi
            self.partnerName = c2sWeekSTKB.partnerName
            self.stkaMsisdn = c2sWeekSTKB.stkaMsisdn
            self.zonePMO = c2sWeekSTKB.zonePMO
            self.regionCom = c2sWeekSTKB.regionCom
            self.regionAdm = c2sWeekSTKB.regionAdm
            self.partnerId = c2sWeekSTKB.partnerId
            

    def __repr__(self):
            return "(id='%s', stkbMsisdn = '%s')> " % (self.id, self.stkbMsisdn)

    def initFromRow(self, row):
        """Cette fonction permet d'initialiser un élément avec une ligne contenue dans le fichier brute,
        les numéros des colonnes doivent être respectés"""

        