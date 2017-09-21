## c2s quotidien par stkb
from sqlalchemy import Column, String, Integer, Float, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from isoweek import Week
from datetime import datetime, date, timedelta
from entity.DDC2SJourSTKB import DDC2SJourSTKB
from db_utils.DBManager import SessionFactory



Base = declarative_base()


class DDC2CWeekSTKP(Base):
    
    """ 
    Cette classe est celle qui permet d'avoir le c2s hebdo par stkb 
    Son fonctionnement est le suivant, le c2s est stocké jour par jour par stkb
    
    //on transfert toutes les données de la table dd_c2s_week_stkb dans dd_c2s_week_stkb_tmp

    Pour chaque jour de la semaine, on selectionne la liste des entrées des stkb qu'on a

        Si une entrée est déjà présente dans la table hebdomadaire, on ne la rajoute plus
        A la fin de cette boucle on est certains d'avoir toutes les stkb présentes dans la semaine
    
    une fois que c'est fait,
    pour chaque ligne de la semaine en question on récupère la stkb ensuite 
        pour chaque jour de la semaine on parcourt les elements pour lesquels stkb =  notre stkb
            en comptant le nombre de jour ou c2s > 0 tout en sommant ce c2s sur la semaine


    """

    _DATE_FORMAT = "%d/%m/%Y"
    
    __tablename__ = 'dd_c2c_sem_stkp'
    id = Column(Integer, primary_key=True, name="id")
    sem =  Column(String(),  name="sem")
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
    c2cMoins1 = Column(Float(), name="c2c_s_1")
    evolutionS1 = Column(Float(), name="evol_s_1")
    objectifC2c = Column(Float(), name="objectif_c2c")

    def __init__(self, stkpDay = None):
        """Si on passe à __init__ un ou plusieurs arguments, le premier doit être une ligne contenant la liste des informations valables
        pour un stkb"""

        if(stkpDay != None):      
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
            self.sem = stkpDay.sem
        
    def __repr__(self):
            return "(id='%s', stkpMsisdn = '%s')> " % (self.id, self.stkpMsisdn)

    def initFromRow(self, row):
        """Cette fonction permet d'initialiser un élément avec une ligne contenue dans le fichier brute,
        les numéros des colonnes doivent être respectés"""

        pass


                  

    
    

                
                


