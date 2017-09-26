# DataLoader est chargée de toutes les actions de lecture dans les fichiers

import csv
from entity.DDC2CJourSTKP import DDC2CJourSTKP
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

  
    

class C2CSTKPDataLoader:

    _DATE_FORMAT = "%d/%m/%Y"
    _SEUIL_PRESENCE_MENSUEL_C2S = 100.0
    _SEUIL_PRESENCE_HEBDO_C2S = 0.0
      
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

    def saveNewC2CJourSTKP(self, otherList):
        
        sessionFactory = SessionFactory()

        session = sessionFactory.Session()

        try:

            session.add_all(otherList)
            session.commit()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion avec succès")  
        except Exception as ex:
            self.view_traceback()
            session.rollback()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion avec erreur, rollback fait!")  
        finally:
            session.close()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Session close")  

    
    
    def insertNewWeek(self, weekString):        

        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Début calculs préalables à l'insertion d'une nouvelle semaine c2s_week_stkb", weekString)  
        annee = int(weekString[:4])        
        # on récupère le numéro de la semaine
        sem = int(weekString[-2:])    

        # Etant donné un numéro de semaine il faut retourner le numéro de semaine qui
        # le précède
        # prevWeek =  
        # 
        
        week = Week(annee, sem)
        lundi = week.monday()

        #on récupère le dimanche
        #
        dimanchePrecedent = lundi + timedelta(days=-1)
        previousYear = dimanchePrecedent.strftime("%Y")

        if(int(previousYear) == annee):
            if(sem - 1) <= 9:
                prevWeekString = previousYear+"0"+str(sem - 1)
            else:
                prevWeekString = previousYear+str(sem-1)
        
        else:
            prevWeekNum = dimanchePrecedent.isocalendar()[1]

            if prevWeekNum <= 9:
                prevWeekString = previousYear + "0"+ str(prevWeekNum)
            else:
                prevWeekString = previousYear+str(prevWeekNum) 

        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin des calculs préalable à l'insertion d'une nouvelle semaine c2s_week_stkb", weekString)
        
        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Chargement des données pour la semaine précédente ", prevWeekString)

        stkpsPreviousWeek = self.getC2CWeek(prevWeekString)

        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Chargement des données pour la semaine en cours ", weekString)
        stkpsWeek = self.getAllStkpForWeek(weekString)

        

        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Début de la complétion des lignes")

        for tmp, val in stkpsWeek.items():
               print(val)

        toAdd = []
        for stkp in stkpsWeek:
            if len(stkpsPreviousWeek) > 0:                
                if(stkpsPreviousWeek.get(stkp) != None):
                    stkpsWeek[stkp].c2cMoins1 = stkpsPreviousWeek[stkp].c2c
                    stkpsWeek[stkp].evolutionS1 = stkpsWeek[stkp].c2c - stkpsPreviousWeek[stkp].c2c 
            toAdd.append(stkpsWeek[stkp])
                                         
                                   
        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin de la complétion des lignes")       
        # à ce niveau ont a toutes les données sur l'évolution et on peut donce enregistrer la nouvelle semaine

     

        sessionFactory = SessionFactory()
        session = sessionFactory.Session()

        try:
            session.add_all(toAdd)
            session.commit()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion avec succès")
        except Exception as ex:
            self.view_traceback()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Echec de l'insertion, annulation des modifications")
            session.rollback()
        finally:
            session.close()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Session close")  
               

    def getC2CWeek(self, weekString):
        """ Cette fonction prend en paramètres une semaine au format aaaass et retourne la liste
        des c2s_stkb_week pour cette semaine """

        sessionFactory = SessionFactory()

        session = sessionFactory.Session()

        stkpsWeek = session.query(DDC2CWeekSTKP).filter_by(sem = weekString)

        resultat = dict()

        for stkp in stkpsWeek:
            resultat[stkp.stkpMsisdn] = stkp

        return resultat

    def getAllStkpForWeek(self, weekString):
        """ Cette fonction prend en paramètres une semaine au format aaaass exemple 201735 
        Génère l'ensemble des jours de la semaine en question au format dd/mm/aaaa 
        
        Elle crée ensuite une liste contenant les informations de la stkb sans c2s """

        #jours = self.getAllWeekDays(weekString, self._DATE_FORMAT)
        jours = Utils.getAllWeekDays(weekString, self._DATE_FORMAT)
        for jour in jours:
            print(jour)
        sessionFactory = SessionFactory()
        session = sessionFactory.Session()
        stkpsWeek = self.getC2CJourSTKPEntriesByDate(jours, self._DATE_FORMAT, session)        
        return stkpsWeek        

    
         


    def getC2CJourSTKPEntriesByDate(self, jours, dateFormat, session):
        """ cette fonction prend en paramètre une date et le format auquel elle est
        Une session et retourne la liste des objets de types DDC2SWeekSTKB ayant pour
        jour stringDate """

        weekData = dict()

        for day in jours:            
            stkps = session.query(DDC2CJourSTKP).filter_by(jour = day)
            for stkp in stkps:
                if(stkp.stkpMsisdn not in weekData):
                    tmpWeekStkp = DDC2CWeekSTKP(stkp)
                    tmpWeekStkp.c2c = 0
                    weekData[stkp.stkpMsisdn] = tmpWeekStkp
                tmp = weekData[stkp.stkpMsisdn]
                tmp.c2c = tmp.c2c + stkp.c2c
                weekData[stkp.stkpMsisdn] = tmp
        return weekData   


    def insertNewMonth(self, monthString):
        
        #on détermine le mois précédent     

        #on récupère tous les jours du mois en paramètre
        year = int(monthString[:4])
        month = int(monthString[-2:])

        sessionFactory = SessionFactory()
        session = sessionFactory.Session()

        if(month == 1): # si nous sommes au premier mois il y a aucune initialisation à faire toutes les valeurs sont à 0
             monthC2C = self.getC2CMoisValues(monthString)
             c2cToAdd = []

             for key, val in monthC2C.items():
                 c2cToAdd.append(val)
        else:
            previousMonth = month -1 

            if(previousMonth <= 9):
                previousMonthString = str(year) + "0" + str(previousMonth)
            else:
                previousMonthString = str(year) + str(previousMonth)
            
            previousMonthC2CList = session.query(DDC2CMoisSTKP).filter_by(mois = previousMonthString)

            previousMonthC2C = dict()

            for prevStkp in previousMonthC2CList:
                previousMonthC2C[prevStkp.stkpMsisdn] = prevStkp

            monthC2C = self.getC2CMoisValues(monthString)

            newMonthC2C = []

            for key in monthC2C:
                
                if(previousMonthC2C.get(key) != None):
                    monthC2C[key].c2cMoins1 = previousMonthC2C[key].c2c
                    monthC2C[key].evolutionM1 = monthC2C[key].c2c - previousMonthC2C[key].c2c
                        #calculer les points de présence
                       #calculer les points d'évolution
                newMonthC2C.append(monthC2C[key])
            

            # a la fin de la boucle on a les C2S mensuels avec les valeurs 
            # il ne reste plus qu'à calculer les points d'assiduité et les points de production

            
            
            #calculer les points de présence
            #calculer les points d'évolution

        try:
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Début insertion ")
            session.add_all(newMonthC2C)
            session.commit()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion ")
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertions avec succès")
        except Exception as ex:
            self.view_traceback()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Echec de l'insertion, annulation des modifications")
            session.rollback()
        finally:
            session.close()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Session close")              
            

    def getC2CMoisValues(self, monthString):
        """ Cette fonction prend en paramètres un mois au format aaaamm
        récupère tous les jours du mois et calcul pour chaque stkb le c2s du mois 
        on crée un dictionnaire contenant toutes les stkbs du mois en question
        au début si un élément n'existe pas on le rajoute, ensuite pour les prochains jours
        """

        #on récupère tous les jours du mois en paramètre
        year = int(monthString[:4])
        month = int(monthString[-2:])

        sessionFactory = SessionFactory()
        session = sessionFactory.Session()

        numDays = calendar.monthrange(year, month)[1]
        days = [datetime.date(year, month, day) for day in range(1, numDays + 1)]
        jours = []
        for day in days:            
            jours.append(day.strftime(self._DATE_FORMAT))
        #on initialise le dictionnaire des données avec 

        c2cMonth = dict()
        for jour in jours:            
            c2cJourStkps = session.query(DDC2CJourSTKP).filter_by(jour = jour)
            for c2cJour in c2cJourStkps:
                if(c2cJour.stkpMsisdn not in c2cMonth):                    
                    tmpC2c = DDC2CMoisSTKP(c2cJour) 
                    tmpC2c.mois = monthString                           
                    tmpC2c.c2c = 0
                    c2cMonth[c2cJour.stkpMsisdn] = tmpC2c
                tmp = c2cMonth[c2cJour.stkpMsisdn]
                tmp.c2c = tmp.c2c + c2cJour.c2c
                c2cMonth[c2cJour.stkpMsisdn] = tmp
        session.close()
        return c2cMonth


    def view_traceback(self):
        ex_type, ex, tb = sys.exc_info()
        traceback.print_tb(tb)
        del tb
                


                
                     

                  
                          
                        
            
        
        


    


    