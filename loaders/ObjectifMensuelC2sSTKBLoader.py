# DataLoader est chargée de toutes les actions de lecture dans les fichiers

import csv
from entity.DDC2CJourSTKP import DDC2CJourSTKP
from entity.DDObjectifMensuelC2sSTKB import DDObjectifMensuelC2sSTKB
from entity.DDObjectifMensuelC2sSTKP import DDObjectifMensuelC2sSTKP
from entity.DDObjectifMensuelC2sSTKA import DDObjectifMensuelC2sSTKA
from entity.DDObjectifMensuelC2sZonePMO import DDObjectifMensuelC2sZonePMO
from entity.DDObjectifMensuelC2sregionAdm import DDObjectifMensuelC2sregionAdm
from entity.DDObjectifMensuelC2sRegionCom import DDObjectifMensuelC2sRegionCom
import time
import calendar
from db_utils.DBManager import SessionFactory
from isoweek import Week
from datetime import date, timedelta
import datetime
import sys, traceback
from entity.DDC2CWeekSTKP import DDC2CWeekSTKP
from entity.DDC2CMoisSTKP import DDC2CMoisSTKP
from commons.Utils import Utils

  
    

class ObjectifMensuelC2sSTKBLoader:

    _DATE_FORMAT = "%d/%m/%Y"
    _SEUIL_PRESENCE_MENSUEL_C2S = 100.0
    _SEUIL_PRESENCE_HEBDO_C2S = 0.0


    def loadDataLocally(self, fileLocation, mois, lignesZebra):
        
        objectifsList = dict()
        resultList = [] 

        with open(fileLocation, "r") as csvfile:

            reader = csv.reader(csvfile, delimiter=";",quotechar='"')
            #on ne tient pas compte de la première ligne
            next(reader)
            next(reader)
            for row in reader:
                print(row)
                tmpElt = DDObjectifMensuelC2sSTKB(row)
                if tmpElt.stkbMsisdn not in objectifsList:       
                    tmpElt.mois = mois               
                    objectifsList[tmpElt.stkbMsisdn] = tmpElt
            
                            
                
        for key, val in objectifsList.items():            
            resultList.append(self.getMappingZebraInfos(val,lignesZebra))        

        return resultList           
    
    def saveObjectifMensuelSTKB(self, objectifsMensuelsSTKB):
        """
        - fileData, les informations telles que chargées du fichier
        - Le mois pour lequel on calcul les objectifs
        - dbSTKBS les stkbs récupérées en BD, plus précisément celles du dernier jour en BD
        """

        sessionFactory = SessionFactory()
        session = sessionFactory.Session()

        try:
            session.add_all(objectifsMensuelsSTKB)
            session.commit()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion avec succès")  
        except Exception as ex:
            self.view_traceback()
            session.rollback()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion avec erreur, rollback fait!")  
        finally:
            session.close()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Session close") 
      
    def loadC2CDayLocally(self, fileLocation, day, mappingZebra):
        """ Cette fonction prend en paramètre une date de jour formatée en chaine de caractère,
        Lit le fichier de configuration pour récupérer le repertoire de dépot des fichiers et lit 
        pour le jour correspondant les données du c2s et les met dans une liste qu'elle renvoie ensuite
        """
        c2cList = dict()
        resultList = []

        with open(fileLocation, "r") as csvfile:
            
            reader = csv.reader(csvfile, delimiter=";",quotechar='"')

            #on ne tient pas compte de la première ligne
            next(reader)
            for row in reader:
                tmpElt = DDC2CJourSTKP(row)
                if tmpElt.stkpMsisdn not in c2cList:                    
                    c2cList[tmpElt.stkpMsisdn] = tmpElt
                else:
                    previousElt = c2cList[tmpElt.stkpMsisdn]
                    previousElt.c2c = previousElt.c2c + tmpElt.c2c
                    c2cList[tmpElt.stkpMsisdn] = previousElt
                                
                                                     
        
        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Début de la complétion totale ...") 
        #semaine = self.getWeekNumberFromDate(day, self._DATE_FORMAT)
        semaine = Utils.getWeekNumberFromDate(day, self._DATE_FORMAT)
        for stkpMsisdn, stkp in c2cList.items():
           stkp.jour = day
           stkp.sem = semaine
           resultList.append(self.getMappingZebraInfos(stkp, mappingZebra))        
                             
        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin de la complétion totale ...") 
        return resultList

    def getMappingZebraInfos(self, other, lignesZebra):
        """ Cette fonction prend en paramètre une ligne c2s et l'ensemble 
        des mapping entre les stkp et les zones pmo, region com, region adm,
        elle parcourt la liste des lignes zebra et test et pour celle qui a le 
        même msisdn elle récupère les informations complémentaires """     
                
        #resultat = dict()
        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Début de la sous complétion ... pour ", other) 
        for ligne in lignesZebra:          
            if ligne.stkpMsisdn.strip() == other.stkpMsisdn.strip():
                print("(",ligne.stkpMsisdn, other.stkpMsisdn,")")
                
                other.partnerName = ligne.partnerName
                other.zonePMO = ligne.zonePMO
                other.regionCom = ligne.regionCom
                other.regionAdm = ligne.regionAdm
                other.partnerId = ligne.partnerId
                other.partnerName = ligne.ddPartnerName        
        

        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin de la sous complétion ... pour ", other) 
        return other



    def insertObjectifsMensuelSTKP(self, month):
        
        sessionFactory = SessionFactory()
        
        data = []

        #01082017
                        
        connection = sessionFactory.getConnection()
        requete = "select stkp_msisdn, stka_msisdn, partner_name, partner_id, region_com, region_adm, zone_pmo, mois, acvi, sum(objectif) objectif from dd_objectif_mensuel_c2s_stkb where mois = '"+month+"' GROUP BY stkp_msisdn"
        result = connection.execute(requete);

        for row in result:
            tmpElt = DDObjectifMensuelC2sSTKP(row) 
            print(tmpElt)     
            data.append(tmpElt)
        connection.close()  

        session = sessionFactory.Session()
        try:
            session.add_all(data)
            session.commit()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion avec succès")  
        except Exception as ex:
            self.view_traceback()
            session.rollback()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion avec erreur, rollback fait!")  
        finally:
            session.close()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Session close") 



    def insertObjectifsMensuelSTKA(self, month):
        
        sessionFactory = SessionFactory()
        
        data = []

        #01082017
                        
        connection = sessionFactory.getConnection()
        requete = "select  stka_msisdn, partner_name, partner_id, region_com, region_adm, mois, acvi, sum(objectif) objectif from dd_objectif_mensuel_c2s_stkb where mois = '"+month+"' GROUP BY stka_msisdn"
        result = connection.execute(requete);

        for row in result:
            tmpElt = DDObjectifMensuelC2sSTKA(row) 
            print(tmpElt)     
            data.append(tmpElt)
        connection.close()  

        session = sessionFactory.Session()
        try:
            session.add_all(data)
            session.commit()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion avec succès")  
        except Exception as ex:
            self.view_traceback()
            session.rollback()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion avec erreur, rollback fait!")  
        finally:
            session.close()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Session close") 



    def insertObjectifsMensuelZonePMO(self, month):
        
        sessionFactory = SessionFactory()
        
        data = []

        #01082017
                        
        connection = sessionFactory.getConnection()
        requete = "select  region_com, region_adm, mois, acvi, zone_pmo sum(objectif) objectif from dd_objectif_mensuel_c2s_stkb where mois = '"+month+"' GROUP BY zone_pmo"
        result = connection.execute(requete);

        for row in result:
            tmpElt = DDObjectifMensuelC2sZonePMO(row) 
            print(tmpElt)     
            data.append(tmpElt)
        connection.close()  

        session = sessionFactory.Session()
        try:
            session.add_all(data)
            session.commit()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion avec succès")  
        except Exception as ex:
            self.view_traceback()
            session.rollback()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion avec erreur, rollback fait!")  
        finally:
            session.close()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Session close") 

    def insertObjectifsMensuelRegionAdm(self, month):
        
        sessionFactory = SessionFactory()
        
        data = []

        #01082017
                        
        connection = sessionFactory.getConnection()
        requete = "select  region_com, region_adm, mois, sum(objectif) objectif from dd_objectif_mensuel_c2s_stkb where mois = '"+month+"' GROUP BY region_adm"
        result = connection.execute(requete);

        for row in result:
            tmpElt = DDObjectifMensuelC2sregionAdm(row) 
            print(tmpElt)     
            data.append(tmpElt)
        connection.close()  

        session = sessionFactory.Session()
        try:
            session.add_all(data)
            session.commit()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion avec succès")  
        except Exception as ex:
            self.view_traceback()
            session.rollback()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion avec erreur, rollback fait!")  
        finally:
            session.close()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Session close") 
        
    def insertObjectifsMensuelRegionCom(self, month):
        
        sessionFactory = SessionFactory()
        
        data = []

        #01082017
                        
        connection = sessionFactory.getConnection()
        requete = "select  region_com, mois, sum(objectif) objectif from dd_objectif_mensuel_c2s_stkb where mois = '"+month+"' GROUP BY region_com"
        result = connection.execute(requete);

        for row in result:
            tmpElt = DDObjectifMensuelC2sregionCom(row) 
            print(tmpElt)     
            data.append(tmpElt)
        connection.close()  

        session = sessionFactory.Session()
        try:
            session.add_all(data)
            session.commit()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion avec succès")  
        except Exception as ex:
            self.view_traceback()
            session.rollback()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion avec erreur, rollback fait!")  
        finally:
            session.close()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Session close")


    


    def view_traceback(self):
        ex_type, ex, tb = sys.exc_info()
        traceback.print_tb(tb)
        del tb
                


                
                     

                  
                          
                        
            
        
        


    


    