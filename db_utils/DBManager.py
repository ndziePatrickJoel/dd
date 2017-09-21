from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class Singleton(type):
    """
    Define an Instance operation that lets clients access its unique
    instance.
    """

    def __init__(cls, name, bases, attrs, **kwargs):
        super().__init__(name, bases, attrs)
        cls._instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class SessionFactory(metaclass=Singleton):
    

    def __init__(self):        
    
        engine = create_engine('mysql+pymysql://beetoo:beetoo@172.21.86.226/dashabord_dd_new?charset=utf8')

        #print(engine.encoding)
        self.Session  = sessionmaker(bind = engine)

        #on instantie une session

    def getSession(self):
        
        session = self.Session()

        return session
    