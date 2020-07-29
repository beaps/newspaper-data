from sqlalchemy import create_engine
# Nos va a permitir tener acceso a las funcionalidades ORM
# Nos permite trabajar con objetos de Python en lugar de queries de SQL directamente
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///newspaper.db')

# Generar el objeto sesión
Session = sessionmaker(bind=engine)

# Generar la clase base de la que extenderán todos los modelos
Base = declarative_base()