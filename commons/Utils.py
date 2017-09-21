import time
import calendar
from db_utils.DBManager import SessionFactory
from isoweek import Week
from datetime import datetime, date, timedelta



class Utils:
    """ Cette classe contient les différentes 
    méthodes qui sont régulièrement utilisées par 
    les autres classes de l'application"""

    @staticmethod
    def getWeekNumberFromDate(self, stringDate, stringFormat, splitDateChar="/"):
                
        normalDate = datetime.strptime(stringDate, stringFormat).date()

        weekNumber = normalDate.isocalendar()[1]

        dateItems = stringDate.split(splitDateChar)

        year = dateItems[len(dateItems) - 1]

        if(weekNumber <= 9):       
            weekNumberStr = str(year) +"0"+str(weekNumber)
        else:
            weekNumberStr = str(year)+str(weekNumber)

            print(weekNumberStr)

        return weekNumberStr
    
    @staticmethod
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