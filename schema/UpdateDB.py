from sqlalchemy import create_engine
from entity.Partenaire import Partenaire
from entity.DDC2SJourSTKB import DDC2SJourSTKB
from entity.meta.DDMappingZebraZPMO import DDMappingZebraZPMO
from sqlalchemy.orm import sessionmaker
from db_utils.DBManager import SessionFactory
import time



class UpdateDB:
    """ Cette classe contient toutes les méthodes qu'on utilise pour 
    mettre à jour la structure de la BD """


    def updateMappingZebra(self):
        
        sessionFactory = SessionFactory()
        session = sessionFactory.getSession()

        partenaires = session.query(Partenaire).all()
        mappingsZebra = session.query(DDMappingZebraZPMO).all()

        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Début du mapping")
        for mapping in mappingsZebra:
            for partenaire in partenaires:
                if(partenaire.designation == mapping.partnerName) or (partenaire.designation in mapping.partnerName) or (mapping.partnerName in partenaire.designation) or (mapping.partnerName.replace(" ", "") in partenaire.designation) or (partenaire.designation.replace(" ", "") in mapping.partnerName):
                    mapping.partnerId = partenaire.id
                    mapping.ddPartnerName = partenaire.designation
        
        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin du mapping")   

        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Début insertion")        
        try:

            session.add_all(mappingsZebra)
            session.commit()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion avec succès")  
        except:
            session.rollback()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion avec erreur, rollback fait!")  
        finally:
            session.close()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Session close")  
                    
