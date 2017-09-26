from sqlalchemy import create_engine
from entity.Partenaire import Partenaire
from entity.DDC2SJourSTKB import DDC2SJourSTKB
from entity.meta.DDMappingZebraZPMO import DDMappingZebraZPMO
from sqlalchemy.orm import sessionmaker
from files_utils.C2SDataLoader import C2SDataLoader
from entity.DDC2SWeekSTKB import DDC2SWeekSTKB
from loaders.C2CSTKPDataLoader import C2CSTKPDataLoader
from loaders.C2CSTKADataLoader import C2CSTKADataLoader
from loaders.C2CPartnerZonePMODataLoader import C2CPartnerZonePMODataLoader
from loaders.C2CPartnerRegionAdmDataLoader import C2CPartnerRegionAdmDataLoader
from loaders.C2CPartnerDataLoader import C2CPartnerDataLoader
from loaders.C2CPartnerRegionComDataLoader import C2CPartnerRegionComDataLoader
from loaders.ObjectifMensuelC2sSTKBLoader import ObjectifMensuelC2sSTKBLoader


engine = create_engine('mysql+pymysql://beetoo:beetoo@172.21.86.226/dashabord_dd_new?charset=utf8')

#print(engine.encoding)
Session  = sessionmaker(bind = engine)

#on instantie une session
session = Session()
#partenaires = session.query(Partenaire).all()

mappingsZebra = session.query(DDMappingZebraZPMO).all()


#for mapping in mappings:
#    print(mapping)


jours = ['02/09/2017', '02/09/2017', '03/09/2017', '04/09/2017', '05/09/2017', '06/09/2017', '07/09/2017', '08/09/2017',
         '09/09/2017','10/09/2017', '11/09/2017', '12/09/2017', '13/09/2017', '14/09/2017', '15/09/2017', '16/09/2017', 
         '17/09/2017','18/09/2017', '19/09/2017', '20/09/2017', '21/09/2017', '22/09/2017', '23/09/2017', '24/09/2017', 
         '25/09/2017','26/09/2017', '27/09/2017', '28/09/2017', '29/09/2017', '30/09/2017', 
         '01/08/2017','02/08/2017', '03/08/2017', '04/08/2017', '05/08/2017', '06/08/2017', '07/08/2017', '08/08/2017', 
         '09/08/2017','10/08/2017', '11/08/2017', '12/08/2017', '13/08/2017', '14/08/2017', '15/08/2017', '16/08/2017', 
         '17/08/2017','18/08/2017', '19/08/2017', '20/08/2017', '21/08/2017', '22/08/2017', '23/08/2017', '24/08/2017', 
         '25/08/2017','26/08/2017', '27/08/2017', '28/08/2017', '29/08/2017', '30/08/2017', '31/08/2017']

#dataLoader = C2CSTKPDataLoader()

#for jour in jours:
#    c2cs = dataLoader.loadC2CDayLocally("c2c_30_08_2017.csv", jour, mappingsZebra)
#    dataLoader.saveNewC2CJourSTKP(c2cs)


#for jour in jours:    
#    stkbs = dataLoader.loadC2SDayLocally("c2s_30_08_2017.csv", jour, mappingsZebra) # on charge la liste des stkb
#    dataLoader.saveNewC2SJourSTB(stkbs)

#testWeek = DDC2SWeekSTKB()

#testWeek.getAllStkbForWeek("201735")

#weeks = ["201731", "201732", "201733", "201734", "201735", "201736", "201737", "201738", "201739"]

#weeks = ["201739"]

#for week in weeks:
#    dataLoader.insertNewWeek(week)

#dataLoader = C2CSTKPDataLoader()
#for week in weeks:
#    dataLoader.insertNewWeek(week)

#dataLoader = C2CSTKPDataLoader()

#mois = ["201708", "201709"]

#dataLoader = C2CPartnerRegionComDataLoader()

#for moi in mois:
#    dataLoader.insertC2CMoisPartnerRegionCom(moi)

#dataLoader.insertC2CWeekPartnerRegionAdm("201731")


#for jour in jours:
#    dataLoader.insertC2CJourPartnerZonePMO(jour)
#dataLoader.insertNewMonth("201709")

#for sem in weeks:
#    dataLoader.insertC2CWeekPartnerZonePMO(sem)

#dataLoader.insertC2CMoisPartnerZonePMO("201708")

#dataLoader.insertC2CSTKAWeek("201734") 08082017

#for sem in weeks:    
#    dataLoader.insertC2CSTKAWeek(sem)


loader = ObjectifMensuelC2sSTKBLoader()

#loadDataLocally(self, fileLocation, mois, lignesZebra)
#objectifs = loader.loadDataLocally("objectifs_stkb_201709.csv", "201709",mappingsZebra)

#loader.saveObjectifMensuelSTKB(objectifs)

loader.insertObjectifsMensuelZonePMO("201709")
loader.insertObjectifsMensuelRegionAdm("201709")
loader.insertObjectifsMensuelRegionCom("201709")

