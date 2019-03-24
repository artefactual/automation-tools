# -*- coding: utf-8 -*-

"""Reingest Model

Enables the configuration and setup of a logging database for monitoring the
process of reingest using the Archivematica AIP.
"""

import datetime
import logging
from os.path import isfile

import enum

from sqlalchemy import create_engine
from sqlalchemy import Column, String, Enum, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

LOGGER = logging.getLogger("transfers")

BASE = declarative_base()


class AIPUUIDException(Exception):
    """Exception class for errors retrieving information about our AIPs from
    the database.
    """

    pass


class StatusEnum(enum.Enum):
    """Controlled list of statuses for recording progress in our database."""

    STATUS_NEW = 1
    STATUS_IN_PROGRESS = 2
    STATUS_COMPLETE = 3
    STATUS_ERROR = 4


class ReingestUnit(BASE):
    """Row definition for the reingest database."""

    __tablename__ = "reingests"
    aip_uuid = Column(String(36), primary_key=True)
    transfer_uuid = Column(String(36))
    status = Column(Enum(StatusEnum))
    message = Column(String(200), nullable=True)
    start_time = Column(DateTime())
    end_time = Column(DateTime())

    def __repr__(self):
        return (
            "uuid={s.aip_uuid}, transfer_uuid={s.transfer_uuid}, "
            "status={s.status}, message={s.message}, "
            "processing_time={s.processing_time}".format(s=self)
        )

    @property
    def processing_time(self):
        """Calculate the processing time of an AIP reingest.

        We look at the start and end time of the process and find the
        difference between the two. This will be an approximation +/- the
        threshold for the cronjob or user's manual update of the database by
        running reingest.py.
        """
        try:
            return "{0} seconds".format(
                int((self.start_time - self.end_time).total_seconds())
            )
        except TypeError:
            LOGGER.error(
                "Date format error getting difference between start "
                "time %s and end time %s",
                self.start_time,
                self.end_time,
            )


def init(databasefile):
    """Initialize the database connection."""
    if not isfile(databasefile):
        # We create the database file here.
        with open(databasefile, "a"):
            pass
    engine = create_engine("sqlite:///{}".format(databasefile), echo=False)
    global Session
    Session = sessionmaker(bind=engine)
    BASE.metadata.create_all(engine)


def get_items(session, status=None):
    """Return everything from the database."""
    if status is None:
        return session.query(ReingestUnit).all()
    return session.query(ReingestUnit).filter_by(status=status).all()


def get_item_by_aip_uuid(session, aip_uuid):
    """Retrieve an item from the database given a specific aip_uuid."""
    return session.query(ReingestUnit).filter_by(aip_uuid=aip_uuid).scalar()


def insert(session, item):
    """Insert an item into the database and update if the item already exists
    and its status is being modified.
    """
    exists = get_item_by_aip_uuid(session, item.aip_uuid)
    if exists is None:
        session.add(item)
        session.commit()
    elif exists.status != item.status:
        LOGGER.info(
            "Item %s exists in database with status %s:", exists.aip_uuid, exists.status
        )


def _set_status(session, status_enum, aip_uuid, transfer_uuid=None, message=None):
    """Setter for status inside the database.

    This function controls various mechanisms for manipulating status.
    """
    item = get_item_by_aip_uuid(session, aip_uuid)
    if item is None:
        raise AIPUUIDException("Cannot find item with UUID %s" % aip_uuid)
    LOGGER.info("setting status %s for AIP %s", status_enum, aip_uuid)
    item.status = status_enum
    if status_enum != StatusEnum.STATUS_ERROR:
        # We can set an existing error message to null if there is an
        # opportunity to correct the execution of the code somehow.
        item.message = ""
    else:
        item.message = message
    if status_enum == StatusEnum.STATUS_IN_PROGRESS and transfer_uuid is not None:
        item.transfer_uuid = transfer_uuid
        item.start_time = datetime.datetime.utcnow()
    if status_enum == StatusEnum.STATUS_COMPLETE:
        item.end_time = datetime.datetime.utcnow()
    session.commit()
    return item


def insert_aip_row_for_reingest(session, aip_uuid):
    """Create a new reingest unit and set item status to new."""
    insert(session, ReingestUnit(aip_uuid=aip_uuid, status=StatusEnum.STATUS_NEW))


def set_status_in_progress(session, aip_uuid, transfer_uuid):
    """Set item status to in progress and update transfer uuid."""
    LOGGER.info("Setting in progress %s, transfer uuid %s", aip_uuid, transfer_uuid)
    _set_status(
        session, StatusEnum.STATUS_IN_PROGRESS, aip_uuid, transfer_uuid=transfer_uuid
    )


def set_status_complete(session, aip_uuid):
    """Set item status to in progress and return processing time."""
    item = _set_status(session, StatusEnum.STATUS_COMPLETE, aip_uuid)
    # Processing_time is an @property of the database item which we can use
    # here to return to the user some logging information that may be of
    # interest.
    time_processed = item.processing_time
    if time_processed is not None:
        LOGGER.info("AIP %s processed in %s", aip_uuid, time_processed)


def set_status_error(session, aip_uuid, message):
    """Set item status to error. If there is an error we want to know about."""
    _set_status(session, StatusEnum.STATUS_ERROR, aip_uuid=aip_uuid, message=message)


def get_items_new(session):
    """Get items in the database that have status new."""
    return get_items(session, StatusEnum.STATUS_NEW)


def get_items_in_progress(session):
    """Get items in the database that have status in progress."""
    return get_items(session, StatusEnum.STATUS_IN_PROGRESS)


def get_items_complete(session):
    """Get items in the database that have status complete."""
    return get_items(session, StatusEnum.STATUS_COMPLETE)


def get_items_error(session):
    """Get items in the database that have error status."""
    return get_items(session, StatusEnum.STATUS_ERROR)
