from sqlalchemy import Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()

class Partenaire(Base):
    """ Cette classe représente un partenaire"""
    __tablename__ = 'tab_partenaires'
    id = Column(Integer, primary_key=True, name = "ID_PARTENAIRE")
    codeFacturation = Column(String(), name="CODE_FACTURATION")
    designation = Column(String(), name="DESIGNATION")
    email = Column(String(), name="EMAIL")
    tel = Column(String(), name="TEL")
    fax = Column(String(), name="FAX")
    adresse = Column(String(), name="ADRESSE")
    siteWeb = Column(String(), name="SITEWEB")
    idRaisonSocial = Column(String(), name="ID_RAISON_SOCIAL")
    idCanal = Column(String(), name="ID_CANAL")


    def __repr__(self):
        return "<Partenaire(id='%s', designation = '%s')> " % (self.id, self.designation)