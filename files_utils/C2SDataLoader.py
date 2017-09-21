# DataLoader est chargée de toutes les actions de lecture dans les fichiers

import csv
from entity.DDC2SJourSTKB import DDC2SJourSTKB
import time
import calendar
from db_utils.DBManager import SessionFactory
from entity.DDC2SWeekSTKB import DDC2SWeekSTKB
from entity.DDC2SMoisSTKB import DDC2SMoisSTKB
from isoweek import Week
from datetime import date, timedelta
import datetime
import sys, traceback


  
    

class C2SDataLoader:

    _DATE_FORMAT = "%d/%m/%Y"
    _SEUIL_PRESENCE_MENSUEL_C2S = 100.0
    _SEUIL_PRESENCE_HEBDO_C2S = 0.0
      
    def loadC2SDayLocally(self, fileLocation, day, mappingZebra):
        """ Cette fonction prend en paramètre une date de jour formatée en chaine de caractère,
        Lit le fichier de configuration pour récupérer le repertoire de dépot des fichiers et lit 
        pour le jour correspondant les données du c2s et les met dans une liste qu'elle renvoie ensuite
        """
        c2sList = []
        resultList = []

        with open(fileLocation, "r") as csvfile:
            
            reader = csv.reader(csvfile, delimiter=";",quotechar='"')

            #on ne tient pas compte de la première ligne
            next(reader)
            nbreRow = 0
            for row in reader:
                c2sList.append(DDC2SJourSTKB(row))
                print("insertion --> ", nbreRow+1)
                nbreRow += 1                                       
        
        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Début de la complétion totale ...") 
        for c2s in c2sList:
           c2s.jour = day
           c2s.sem = self.getWeekNumberFromDate(day, self._DATE_FORMAT)
           resultList.append(self.getMappingZebraInfos(c2s, mappingZebra))        
                             
        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin de la complétion totale ...") 
        return resultList

    def getMappingZebraInfos(self, c2s, lignesZebra):
        """ Cette fonction prend en paramètre une ligne c2s et l'ensemble 
        des mapping entre les stkp et les zones pmo, region com, region adm,
        elle parcourt la liste des lignes zebra et test et pour celle qui a le 
        même msisdn elle récupère les informations complémentaires """     
                
        #resultat = dict()
        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Début de la sous complétion ... pour ", c2s) 
        for ligne in lignesZebra:          
            if ligne.stkpMsisdn.strip() == c2s.stkpMsisdn.strip():
                print("(",ligne.stkpMsisdn, c2s.stkpMsisdn,")")
                
                c2s.partnerName = ligne.partnerName
                c2s.zonePMO = ligne.zonePMO
                c2s.regionCom = ligne.regionCom
                c2s.regionAdm = ligne.regionAdm
                c2s.partnerId = ligne.partnerId
                c2s.partnerName = ligne.ddPartnerName

        
        

        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin de la sous complétion ... pour ", c2s) 
        return c2s

    def saveNewC2SJourSTB(self, c2sList):
        
        sessionFactory = SessionFactory()

        session = sessionFactory.Session()

        try:

            session.add_all(c2sList)
            session.commit()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion avec succès")  
        except Exception as ex:
            self.view_traceback()
            session.rollback()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin insertion avec erreur, rollback fait!")  
        finally:
            session.close()
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Session close")  

    
    def getWeekNumberFromDate(self, stringDate, stringFormat, splitDateChar="/"):
        
        
        normalDate = datetime.datetime.strptime(stringDate, stringFormat).date()

        weekNumber = normalDate.isocalendar()[1]

        dateItems = stringDate.split(splitDateChar)

        year = dateItems[len(dateItems) - 1]

        if(weekNumber <= 9):       
            weekNumberStr = str(year) +"0"+str(weekNumber)
        else:
            weekNumberStr = str(year)+str(weekNumber)

            print(weekNumberStr)

        return weekNumberStr



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

        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin des calculs préalables à l'insertion d'une nouvelle semaine c2s_week_stkb", weekString)

        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Chargement des données pour la semaine précédente")

        stkbsPreviousWeek = self.getC2SWeek(prevWeekString)

        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Chargement des données pour la semaine en cours")
        stkbsWeek = self.getAllStkbForWeek(weekString)

        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Début de la complétion des lignes")

        stkbsToAdd = []

        for stkb in stkbsWeek:
            print(time.strftime("%d/%m/%Y %H:%M:%S"), ">>>> Début de la complétion pour", stkb)
            stkbPrev = stkbsPreviousWeek.get(stkb)
            if stkbPrev != None:
                stkbsWeek[stkb].c2sMoins1 = stkbPrev.c2s
                stkbsWeek[stkb].evolutionS1 = stkbsWeek[stkb].c2s - stkbPrev.c2s            
            stkbsToAdd.append(stkbsWeek[stkb])
                
        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin de la complétion des lignes")
        

        # à ce niveau ont a toutes les données sur l'évolution et on peut donce enregistrer la nouvelle semaine

        sessionFactory = SessionFactory()
        session = sessionFactory.Session()
        
        try:
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Début insertion ")
            session.add_all(stkbsToAdd)
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
               

    def getC2SWeek(self, weekString):
        """ Cette fonction prend en paramètres une semaine au format aaaass et retourne la liste
        des c2s_stkb_week pour cette semaine """

        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Début chargement de la semaine précédente (", weekString, ")")

        sessionFactory = SessionFactory()

        session = sessionFactory.Session()

        stkbsWeek = session.query(DDC2SWeekSTKB).filter_by(sem = weekString)

        result = dict()

        for stkb in stkbsWeek:
            result[stkb.stkbMsisdn] = stkb

        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Fin chargement de la semaine précédente (", weekString, ")")

        return result

    def getAllStkbForWeek(self, weekString):
        """ Cette fonction prend en paramètres une semaine au format aaaass exemple 201735 
        Génère l'ensemble des jours de la semaine en question au format dd/mm/aaaa 
        
        Elle crée ensuite une liste contenant les informations de la stkb sans c2s """

        jours = self.getAllWeekDays(weekString, self._DATE_FORMAT)

        sessionFactory = SessionFactory()

        session = sessionFactory.Session()

        stkbsWeek = self.getC2SJourSTKBEntriesByDate(jours, self._DATE_FORMAT, session)

        session.close()
        
        return stkbsWeek        

    
    def getAllWeekDays(self, weekString, dateFormat):
        """ Cette fonction prend en paramètre une semaine au format aaaass et 
        retourne la liste des jours de cette semaine au format spécifié 
        """

        #on récupère l'année c'est à dire les 4 premiers caractères
        annee = int(weekString[:4])
        
        # on récupère le numéro de la semaine
        sem = int(weekString[-2:])

        week = Week(annee, sem)
        lundi = week.monday()        

        jours = []

        jours.append(lundi.strftime(dateFormat))

        for i in range(1, 7):
            tmpDate = lundi + timedelta(days=i)
            jours.append(tmpDate.strftime(dateFormat))

        return jours

        


    def getC2SJourSTKBEntriesByDate(self, jours, dateFormat, session):
        """ cette fonction prend en paramètre une date et le format auquel elle est
        Une session et retourne la liste des objets de types DDC2SWeekSTKB ayant pour
        jour stringDate """
        weekData = dict()
        for day in jours:   
            print(time.strftime("%d/%m/%Y %H:%M:%S"), ">>>> exécution pour le jour  ", day)  
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "******** Chargement des C2SJourSTKB ")       
            stkbs = session.query(DDC2SJourSTKB).filter_by(jour = day)
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "******** Fin Chargement des C2SJourSTKB ")
            print(time.strftime("%d/%m/%Y %H:%M:%S"), ">>>> début de parcours pour le jour  ", day) 
            for stkb in stkbs:
                print(time.strftime("%d/%m/%Y %H:%M:%S"), "******** Début de test pour  ", stkb)
                if(stkb.stkbMsisdn not in weekData):
                    tmpWeekStkb = DDC2SWeekSTKB(stkb)
                    tmpWeekStkb.c2s = 0
                    weekData[stkb.stkbMsisdn] = tmpWeekStkb
                tmp = weekData[stkb.stkbMsisdn]
                tmp.c2s = tmp.c2s + stkb.c2s
                if(stkb.c2s > self._SEUIL_PRESENCE_HEBDO_C2S):
                    if tmp.presence != None:
                        tmp.presence = tmp.presence + 1
                    else:
                        tmp.presence = 1
                else:
                    if tmp.presence == None:
                        tmp.presence = 0
                weekData[stkb.stkbMsisdn] = tmp
                print(time.strftime("%d/%m/%Y %H:%M:%S"), "******** Fin de test pour  ", stkb)            
            print(time.strftime("%d/%m/%Y %H:%M:%S"), ">>>> Fin de parcours pour ", day)
        return weekData   


    def insertNewMonth(self, monthString):
        
        #on détermine le mois précédent

        sessionFactory = SessionFactory()

        session = sessionFactory.Session()

        #on récupère tous les jours du mois en paramètre
        year = int(monthString[:4])
        month = int(monthString[-2:])

        if(month == 1): # si nous sommes au premier mois il y a aucune initialisation à faire toutes les valeurs sont à 0
             monthC2S = self.getC2SMoisValues(monthString)
             c2sToAdd = []

             for key, val in monthC2S.items():
                 c2sToAdd.apppend(val)
        else:
            previousMonth = month -1 

            if(previousMonth <= 9):
                previousMonthString = str(year) + "0" + str(previousMonth)
            else:
                previousMonthString = str(year) + str(previousMonth)

            print(time.strftime("%d/%m/%Y %H:%M:%S"), " >>> début de la récupération des données du mois précédent ", previousMonthString)
            
            previousMonthC2SList = session.query(DDC2SMoisSTKB).filter_by(mois = previousMonthString)

            print(time.strftime("%d/%m/%Y %H:%M:%S"), ">>> Fin de la récupération des données du mois précédent ")

            previousMonthC2S = dict()

            for prevStkb in previousMonthC2SList:
                previousMonthC2S[prevStkb.stkbMsisdn] = prevStkb
            
            print(time.strftime("%d/%m/%Y %H:%M:%S"), ">>> Récupération des données du mois en cours ", monthString)
            monthC2S = self.getC2SMoisValues(monthString)
            print(time.strftime("%d/%m/%Y %H:%M:%S"), ">>> Fin de la récupération des données du mois en cours ")

            newMonthC2S = []

            print(time.strftime("%d/%m/%Y %H:%M:%S"), ">>> Début de la complétion des informations ")
            for key in monthC2S:
                print(time.strftime("%d/%m/%Y %H:%M:%S"), ">>>>>> Début de la complétion des informations pour  ", key)
                if(previousMonthC2S.get(key) != None):
                    monthC2S[key].c2sMoins1 = previousMonthC2S[key].c2s
                    monthC2S[key].evolutionM1 = monthC2S[key].c2s - previousMonthC2S[key].c2s
                        #calculer les points de présence
                        #calculer les points d'évolution
                    
                    if(previousMonthC2S[key].c2s != 0 and previousMonthC2S[key] != None):
                        evolution  = (monthC2S[key].c2s / previousMonthC2S[key].c2s)

                        if(evolution < 1):
                            pointsEvolution = -20
                        if(evolution == 1):
                            pointsEvolution = 5
                        if(evolution > 1 and evolution < 1.1):
                            pointsEvolution = 10
                        if(evolution >= 1.1 and evolution < 1.2):
                            pointsEvolution = 20
                        if(evolution >= 1.2):
                            pointsEvolution = 25                        
                        monthC2S[key].pointsEvol = pointsEvolution 

                    else:
                        monthC2S[key].pointsEvol = 0 
                print(time.strftime("%d/%m/%Y %H:%M:%S"), ">>>>>> Fin de la complétion des informations pour  ", key)          
                newMonthC2S.append(monthC2S[key])            

            # a la fin de la boucle on a les C2S mensuels avec les valeurs 
            # il ne reste plus qu'à calculer les points d'assiduité et les points de production

            c2sToAdd = []

            for c2s in newMonthC2S:
                
                presence = c2s.presence

                if presence < 20 : 
                    pointsPresence = 0
                elif presence >= 20 and presence < 25 : 
                    pointsPresence = 10
                elif presence >= 25:
                    pointsPresence = 25

                c2s.pointsAssiduite = pointsPresence
                # calcul des points de production
                c2s.pointsProd = (c2s.c2s / 10000) * 0.5             
                c2sToAdd.append(c2s)          
            
            #calculer les points de présence
            #calculer les points d'évolution
        sessionFactory = SessionFactory()
        session = sessionFactory.Session()
        
        try:
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Début insertion ")
            session.add_all(c2sToAdd)
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

    def getC2SMoisValues(self, monthString):
        """ Cette fonction prend en paramètres un mois au format aaaamm
        récupère tous les jours du mois et calcul pour chaque stkb le c2s du mois 
        on crée un dictionnaire contenant toutes les stkbs du mois en question
        au début si un élément n'existe pas on le rajoute, ensuite pour les prochains jours
        """

        #on récupère tous les jours du mois en paramètre
        year = int(monthString[:4])
        month = int(monthString[-2:])

        numDays = calendar.monthrange(year, month)[1]

        days = [datetime.date(year, month, day) for day in range(1, numDays + 1)]

        sessionFactory = SessionFactory()
        session = sessionFactory.Session()
        

        jours = []

        for day in days:            
            jours.append(day.strftime(self._DATE_FORMAT))

        #on initialise le dictionnaire des données avec 

        c2sMonth = dict()

        for jour in jours:
            print(time.strftime("%d/%m/%Y %H:%M:%S"), ">>>>>>>>> Début recherche infos pour le jour ", jour)            
            c2sJourStkbs = session.query(DDC2SJourSTKB).filter_by(jour = jour)           

            for c2sJour in c2sJourStkbs:

                if(c2sJour.stkbMsisdn not in c2sMonth):

                    tmpC2s = DDC2SMoisSTKB(c2sJour)      
                    tmpC2s.mois = monthString                            
                    tmpC2s.c2s = 0
                    c2sMonth[c2sJour.stkbMsisdn] = tmpC2s                

                tmp = c2sMonth[c2sJour.stkbMsisdn]

                tmp.c2s = tmp.c2s + c2sJour.c2s

                if c2sJour.c2s >= self._SEUIL_PRESENCE_MENSUEL_C2S:
                    if tmp.presence != None:
                       tmp.presence = tmp.presence + 1
                    else:
                       tmp.presence = 1
                else:
                    if tmp.presence == None:
                        tmp.presence = 0
            print(time.strftime("%d/%m/%Y %H:%M:%S"), " >>>>>>>>> Fin recherche infos pour le jour ", jour)

        return c2sMonth

    def view_traceback(self):
        ex_type, ex, tb = sys.exc_info()
        traceback.print_tb(tb)
        del tb
                


                
                     

                  
                          
                        
            
        
        


    


    