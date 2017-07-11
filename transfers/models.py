from sqlalchemy import create_engine
from sqlalchemy import Sequence
from sqlalchemy import Column, Binary, Boolean, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class Unit(Base):
    __tablename__ = 'unit'
    id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
    uuid = Column(String(36))
    path = Column(Binary())
    unit_type = Column(String(10))  # ingest or transfer
    status = Column(String(20), nullable=True)
    microservice = Column(String(50))
    current = Column(Boolean(create_constraint=False))
    started_timestamp = Column(DateTime(timezone=True))  # UTC time started

    def __repr__(self):
        return "<Unit(id={s.id}, uuid={s.uuid}, unit_type={s.unit_type}, path={s.path}, status={s.status}, current={s.current}, timestamp={s.started_timestamp})>".format(s=self)


def init(databasefile):
    engine = create_engine('sqlite:///{}'.format(databasefile), echo=False)
    global Session
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
