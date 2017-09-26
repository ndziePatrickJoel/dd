
from entity.DDC2CJourSTKA import DDC2CJourSTKA
from entity.DDC2CJourSTKP import DDC2CJourSTKP
from entity.DDC2CSemSTKA import DDC2CSemSTKA
from entity.DDC2CMoisSTKA import DDC2CMoisSTKA
from entity.STKA import STKA
import time
import calendar
from db_utils.DBManager import SessionFactory
from isoweek import Week
from datetime import date, timedelta
import datetime
import sys, traceback
from commons.Utils import Utils


class C2CSTKADataLoader:

    _DATE_FORMAT = '%d/%m/%Y'


    def insertC2CSTKADay(self, day):
        
        sessionFactory = SessionFactory()

        session = sessionFactory.Session()

        stkps = session.query(DDC2CJourSTKP).filter_by(jour = day)

        resultat = dict()

        sem = Utils.getWeekNumberFromDate(day, self._DATE_FORMAT)

        for stkp in stkps:
            if(resultat.get(stkp.stkaMsisdn) == None):                
                tmp = DDC2CJourSTKA(stkp)
                tmp.jour = day
                tmp.sem = sem
                tmp.stkaMsisdn = stkp.stkaMsisdn
                resultat[stkp.stkaMsisdn] = tmp       
                
        connection = sessionFactory.getConnection()
        result = connection.execute("select sum(c2c) c2c, stka_msisdn  from dd_c2c_jour_stkp where jour= '"+day+"' group by stka_msisdn")

        for row in result:            
            stkaMsisdn = row['stka_msisdn']
            tmpC2c = row['c2c']
            resultat[stkaMsisdn].c2c = tmpC2c

        connection.close()    

        toAdd = []

        for res, val in resultat.items():
            toAdd.append(val)

        try:
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Début insertion ")
            session.add_all(toAdd)
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


    def getC2CSTKAWeek(self, weekString):
        """ Cette fonction prend en paramètre une semaine et calcule pour chaque stka en BD
        son C2C pour cette semaine """
        
        weekDays = Utils.getAllWeekDays(weekString, self._DATE_FORMAT)
        resultat = dict()
        sessionFactory = SessionFactory()
        session = sessionFactory.Session()

        print(time.strftime("%d/%m/%Y %H:%M:%S"), ">>>>>>>>>> Début du parcours des jours pour la semaine ", weekString)
        for day in weekDays:
            stkasDay = session.query(DDC2CJourSTKA).filter_by(jour = day)
            print(time.strftime("%d/%m/%Y %H:%M:%S"),">>>>>>>>>>>>>>>>>>> Début du parcours pour le jour ", day )                
            for stka in stkasDay:
                if(resultat.get(stka.stkaMsisdn) == None):                
                    resultat[stka.stkaMsisdn] = DDC2CSemSTKA(stka)                
                else:
                    resultat[stka.stkaMsisdn].c2c = resultat[stka.stkaMsisdn].c2c + stka.c2c
            print(time.strftime("%d/%m/%Y %H:%M:%S"),">>>>>>>>>>>>>>>>>>> Fin du parcours pour le jour ", day )
        

        return resultat

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
            
            previousMonthC2CList = session.query(DDC2CMoisSTKA).filter_by(mois = previousMonthString)

            previousMonthC2C = dict()

            for prevStkp in previousMonthC2CList:
                previousMonthC2C[prevStkp.stkaMsisdn] = prevStkp

            monthC2C = self.getC2CMoisValues(monthString)

            newMonthC2C = []

            for key in monthC2C:
                
                if(previousMonthC2C.get(key) != None):
                    monthC2C[key].c2cM1 = previousMonthC2C[key].c2c
                    monthC2C[key].evolM1 = float(monthC2C[key].c2c) - float(previousMonthC2C[key].c2c)
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

        numDays = calendar.monthrange(year, month)[1]
        days = [datetime.date(year, month, day) for day in range(1, numDays + 1)]
        jours = []
        for day in days:            
            jours.append(day.strftime(self._DATE_FORMAT))
        #on initialise le dictionnaire des données avec 

        sessionFactory = SessionFactory()
        session = sessionFactory.Session()

        c2cMonth = dict()
        for jour in jours:            
            c2cJourStkas = session.query(DDC2CJourSTKA).filter_by(jour = jour)
            for c2cJour in c2cJourStkas:
                if(c2cJour.stkaMsisdn not in c2cMonth):                    
                    tmpC2c = DDC2CMoisSTKA(c2cJour) 
                    tmpC2c.mois = monthString                           
                    tmpC2c.c2c = 0
                    c2cMonth[c2cJour.stkaMsisdn] = tmpC2c
                tmp = c2cMonth[c2cJour.stkaMsisdn]
                tmp.c2c = tmp.c2c + c2cJour.c2c
                c2cMonth[c2cJour.stkaMsisdn] = tmp
        session.close()
        return c2cMonth

    def insertC2CSTKAWeek(self, weekString):
        
        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Début calculs préalables à l'insertion d'une nouvelle semaine dans c2CSTKA", weekString)  
        annee = int(weekString[:4])        
        # on récupère le numéro de la semaine
        sem = int(weekString[-2:])    

        week = Week(annee, sem)
        lundi = week.monday()

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

        print(time.strftime("%d/%m/%Y %H:%M:%S"), "Récupération de la semaine précédente", prevWeekString) 

        sessionFactory = SessionFactory()
        session = sessionFactory.Session()
        print(time.strftime("%d/%m/%Y %H:%M:%S"), ">>>>>>>> Récupération des informations de la semaine précédente ", prevWeekString)
        stkasPreviousWeekList = session.query(DDC2CSemSTKA).filter_by(sem = prevWeekString)
        print(time.strftime("%d/%m/%Y %H:%M:%S"), ">>>>>>>> Fin Récupération des informations de la semaine précédente ", prevWeekString)
        

        stkasPreviousWeek = dict()

        for stkaPrev in stkasPreviousWeekList:
            stkasPreviousWeek[stkaPrev.stkaMsisdn] = stkaPrev

        print(time.strftime("%d/%m/%Y %H:%M:%S"), ">>>>>>>> Récupération des informations de la semaine en cours ", weekString)
        stkasWeek = self.getC2CSTKAWeek(weekString)
        print(time.strftime("%d/%m/%Y %H:%M:%S"), ">>>>>>>> Fin de la récupération des informations de la semaine en cours ", weekString)
        toAdd = []

        if len(stkasPreviousWeek) > 0:
            #si le tableau de la semaine précédente n'est pas vide
            #on met à jour les informations relatives au mois précédent
            for stka, val in stkasWeek.items():
                if stkasPreviousWeek.get(stka) != None:
                    val.c2cS1 = stkasPreviousWeek.get(stka).c2c
                    val.evolS1 = float(val.c2c) - float(stkasPreviousWeek.get(stka).c2c)                
                toAdd.append(val)    
        else:
            for stka, val in stkasWeek.items():
                toAdd.append(val)           
                 
        try:
            print(time.strftime("%d/%m/%Y %H:%M:%S"), "Début insertion ")
            session.add(toAdd)
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

    
    def view_traceback(self):
        ex_type, ex, tb = sys.exc_info()
        traceback.print_tb(tb)
        del tb