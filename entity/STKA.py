from sqlalchemy import Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()

class STKA(Base):
    """ Cette classe représente une stka, une stka fait référence à un partenaire dans le référentiel"""
    __tablename__ = 'vue_stka'    
    stkaMsisdn = Column(String(), name="stka_msisdn")
    partnerId = Column(String(), name="partner_id")
    partnerName = Column(String(), name="partner_name")
    zonePMO = Column(String(), name="zone_pmo")
    regionAdm = Column(String(), name="region_administrative")
    regionCom = Column(String(), name="region_commerciale")
    partnerId = Column(String(), name="partner_id")
    ddPartnerName = Column(String(), name="dd_partner_name")
   

    def __repr__(self):
        return "<stka(id='%s', stkaMsisdn = '%s', partnerId = '%s')> " % (self.id, self.stkaMsisdn, self.partnerId)