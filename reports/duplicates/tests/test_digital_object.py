#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
import os
import sys

duplicates_module = os.path.dirname((os.path.dirname(os.path.abspath(__file__))))
sys.path.append(duplicates_module)


from digital_object import DigitalObject


def test_equality():
    """do something."""

    test_path = "data/objects/sub-dir-1/2"
    obj = DigitalObject()
    obj.set_basename(test_path)
    obj.set_dirname(test_path)
    obj.filepath = test_path
    obj.hashes = {"d41d8cd98f00b204e9800998ecf8427e": "md5"}
    obj.date_modified = "2018-08-14"

    assert obj == obj
    assert obj % obj == 0

    new_obj = copy.copy(obj)
    new_obj.date_modified = "2018-08-16"

    assert new_obj != obj
    assert new_obj % obj != 0
