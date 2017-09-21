## mapping zebra zones pmo

from sqlalchemy import Column, String, Integer, Float, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class DDMappingZebraZPMO(Base):
    __tablename__ = 'dd_mapping_zebra_pmo'
    id = Column(Integer, primary_key=True, name="id")
    stkpName = Column(String(), name="stkp_name")
    stkpMsisdn = Column(String(), name="stkp_msisdn")
    stkaName = Column(String(), name="stka_name")
    partnerName = Column(String(), name="partner_name")
    partnerId = Column(String(), name="partner_id")
    stkaMsisdn = Column(String(), name="stka_msisdn")
    zonePMO = Column(String(), name="zone_pmo")
    regionAdm = Column(String(), name="region_administrative")
    regionCom = Column(String(), name="region_commerciale")
    ddPartnerName = Column(String(), name="dd_partner_name")
    
    

    def __repr__(self):
            return "(id='%s', stkpMsisdn = '%s')> " % (self.id, self.stkpMsisdn)

    def initFromRow(self, row):
        """Cette fonction permet d'initialiser un élément avec une ligne contenue dans le fichier brute,
        les numéros des colonnes doivent être respectés"""

        