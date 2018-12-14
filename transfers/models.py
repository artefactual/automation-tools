# -*- coding: utf-8 -*-
from sqlalchemy import create_engine
from sqlalchemy import Sequence
from sqlalchemy import Column, LargeBinary, Boolean, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

Base = declarative_base()
Session = None
transfer_session = None


class Unit(Base):
    """Object that represents transfer units in the automation tools database.
    """
    __tablename__ = 'unit'

    id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
    uuid = Column(String(36))
    path = Column(LargeBinary())
    unit_type = Column(String(10))  # ingest or transfer
    status = Column(String(20), nullable=True)
    microservice = Column(String(50))
    current = Column(Boolean(create_constraint=False))

    def __repr__(self):
        return ("<Unit(id={s.id}, uuid={s.uuid}, unit_type={s.unit_type}, "
                "path={s.path}, status={s.status}, current={s.current})>"
                .format(s=self))


def init_session(databasefile):
    """Initialize the database given a database filename and initiate the
    database session to use throughout our transactions.
    """
    engine = create_engine('sqlite:///{}'.format(databasefile), echo=False)
    global Session
    Session = scoped_session(sessionmaker())
    Session.configure(bind=engine)
    global transfer_session
    transfer_session = Session()
    Base.metadata.create_all(engine)


def cleanup_session():
    """Call remove to clean up the current session's transactions."""
    Session.remove()


def get_current_unit():
    """Query the database for current units. Return the first."""
    return transfer_session.query(Unit).filter_by(current=True).one()


def get_processed_transfer_paths():
    """Return a set that represents the processed transfer paths in the
    database. Set is a set of all paths in the database. The caller needs to
    create a delta by comparing the result to other data points, e.g. a list of
    paths of its own.
    """
    return {x[0] for x in transfer_session.query(Unit.path).all()}


def retrieve_unit_by_type_and_uuid(uuid, unit_type):
    """Given a unit_type and uuid for that unit, return the unit object that
    represents it.
    """
    return transfer_session.query(Unit).\
        filter_by(unit_type=unit_type, uuid=uuid).one()


def _update_unit(uuid, path, unit_type, status, current, microservice=""):
    """Internal function to handle the updating of a unit in the database as
    a single atomic transaction.
    """
    unit = Unit(
        uuid=uuid, path=path, unit_type=unit_type, status=status,
        current=current, microservice=microservice)
    transfer_session.add(unit)
    transfer_session.commit()
    return unit


def add_new_transfer(uuid, path):
    """Add a new transfer unit to the database."""
    return _update_unit(
        uuid=uuid, path=path, unit_type="transfer", status="",
        current=True)


def transfer_failed_to_start(path):
    """Update a unit when its transfer has failed to start."""
    _update_unit(
        uuid="", path=path, unit_type="transfer", status="FAILED",
        current=False)


def failed_to_approve(path):
    """Update a unit when it has failed to be approved by the automation
    tools.
    """
    return _update_unit(
        uuid=None, path=path, unit_type="transfer", status="",
        current=False)


def update_unit_type_and_uuid(unit, unit_type, uuid):
    """Update the unit_type and uuid for a unit, e.g. when a transfer unit
    becomes a SIP within the ingest workflow.
    """
    unit.unit_type = unit_type
    unit.uuid = uuid
    transfer_session.commit()


def update_unit_microservice(unit, microservice):
    """Update the microservice column of the given unit."""
    unit.microservice = microservice
    transfer_session.commit()


def update_unit_current(unit, current):
    """Update the 'current' parameter of the given unit, e.g. when the unit
    is still current and needs to be processed by the automation tools.
    """
    unit.current = current
    transfer_session.commit()


def update_unit_status(unit, status):
    """Update the status of the given unit, e.g. COMPLETED, PROCESSING, etc."""
    unit.status = status
    transfer_session.commit()
