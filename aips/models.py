from os.path import isfile

from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import Integer
from sqlalchemy import Sequence
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class Aip(Base):
    __tablename__ = "aip"
    id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
    uuid = Column(String(36), nullable=False, unique=True)

    def __repr__(self):
        return f"Aip(id={self.id!r}, uuid={self.uuid!r})"


def init(databasefile):
    if not isfile(databasefile):
        with open(databasefile, "a"):
            pass
    engine = create_engine(f"sqlite:///{databasefile}", echo=False)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)
    return session()
