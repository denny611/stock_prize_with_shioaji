from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData

SQL_DB="t.db"
engine = create_engine(f'sqlite:///{SQL_DB}' , echo = True)
meta = MetaData()

students = Table(
   'students', meta, 
   Column('id', Integer, primary_key = True), 
   Column('name', String), 
   Column('lastname', String), 
   )
meta.create_all(engine)
