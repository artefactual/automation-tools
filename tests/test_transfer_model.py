#!/usr/bin/env python
# -*- coding: utf-8 -*-
from uuid import uuid4

import pytest
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.orm.session import Session

from transfers import models


@pytest.fixture
def setup_session():
    """Cleanup any previous transaction and initialize a new database and
    session to work with."""
    if models.Session:
        models.cleanup_session()
        models.Session = None
    models.init_session(":memory:")
    try:
        models.transfer_session.query(models.Unit).one()
    except MultipleResultsFound:
        assert False
    except NoResultFound:
        assert True


def test_database_init():
    """Test the initialization of the database. Ensure that we return a
    Session object. It would be very easy to return a scoped_session object
    and otherwise start using that."""
    assert models.transfer_session is None
    models.init_session(":memory:")
    assert isinstance(models.transfer_session, Session)


def test_get_functions(setup_session):
    """Test the various get functions of the models module."""
    transfer_one_uuid = str(uuid4())
    models._update_unit(
        uuid=transfer_one_uuid,
        path=b"/foo",
        unit_type="transfer",
        status="PROCESSING",
        current=True,
    )
    unit = models.get_current_unit()
    assert unit.uuid == transfer_one_uuid
    transfer_two_uuid = str(uuid4())
    models._update_unit(
        uuid=transfer_two_uuid,
        path=b"/bar",
        unit_type="ingest",
        status="COMPLETE",
        current=False,
    )
    unit_two = models.retrieve_unit_by_type_and_uuid(
        unit_type="ingest", uuid=transfer_two_uuid
    )
    unit_one = models.retrieve_unit_by_type_and_uuid(
        unit_type="transfer", uuid=transfer_one_uuid
    )
    assert unit_one.uuid == transfer_one_uuid
    assert unit_two.uuid == transfer_two_uuid
    all_processed_paths = models.get_processed_transfer_paths()
    assert len(all_processed_paths) == 2


def test_start_Transfer_unit_state(setup_session):
    """Test functions associated with setting unit state at the beginning of a
    new automated transfer.
    """
    transfer_uuid_one = str(uuid4())
    models.add_new_transfer(uuid=transfer_uuid_one, path=b"/foo")
    unit = models.transfer_session.query(models.Unit).filter_by(path=b"/foo").one()
    models.transfer_failed_to_start(path=b"/bar")
    unit = models.transfer_session.query(models.Unit).filter_by(path=b"/bar").one()
    models.failed_to_approve(b"/foobar")
    assert unit.current is False
    assert unit.uuid == ""
    assert unit.status == "FAILED"
    unit = models.transfer_session.query(models.Unit).filter_by(path=b"/foobar").one()
    assert unit.current is False
    assert unit.uuid is None


def test_update_unit_attributes(setup_session):
    """Test functions associated with updating object attributes in the
    database.
    """
    transfer_one_uuid = str(uuid4())
    ingest_one_uuid = str(uuid4())
    models._update_unit(
        uuid=transfer_one_uuid,
        path=b"/foo",
        unit_type="transfer",
        status="PROCESSING",
        current=True,
    )
    unit = models.transfer_session.query(models.Unit).one()
    assert unit.uuid == transfer_one_uuid
    assert unit.unit_type == "transfer"
    assert unit.microservice == ""
    assert unit.current is True
    assert unit.status == "PROCESSING"
    models.update_unit_type_and_uuid(
        unit=unit, unit_type="ingest", uuid=ingest_one_uuid
    )
    models.update_unit_microservice(
        unit=unit, microservice="Generate METS.xml document"
    )
    models.update_unit_current(unit=unit, current=False)
    models.update_unit_status(unit=unit, status="COMPLETE")
    unit = models.transfer_session.query(models.Unit).one()
    assert unit.uuid == ingest_one_uuid
    assert unit.unit_type == "ingest"
    assert unit.microservice == "Generate METS.xml document"
    assert unit.current is False
    assert unit.status == "COMPLETE"
