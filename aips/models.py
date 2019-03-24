from os.path import isfile

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, Sequence, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class Aip(Base):
    __tablename__ = "aip"
    id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
    uuid = Column(String(36), nullable=False, unique=True)

    def __repr__(self):
        return "Aip(id=%r, uuid=%r)" % (self.id, self.uuid)


def init(databasefile):
    if not isfile(databasefile):
        with open(databasefile, "a"):
            pass
    engine = create_engine("sqlite:///{}".format(databasefile), echo=False)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)
    return session()
