#!/usr/bin/env python
# -*- coding: utf-8 -*-

import hashlib


# Checksum algorithms to test for based on allowed bag compressions in
# Archivematica.
class Hashes(object):
    """Hashes provides us with a convenient method to initialize a new list
    of hashlib hash types to compile checksums for an object against and make
    sure that state isn't maintained between initialization.
    """

    def __init__(self):
        """Initialize a group of hash types to generate checksums with."""
        self.checksum_functions = [
            hashlib.md5(),
            hashlib.sha1(),
            hashlib.sha256(),
            hashlib.sha512(),
        ]
        self.checksum_algorithms = [
            algorithm.name for algorithm in self.checksum_functions
        ]


def hash(fname):
    """Run all the hashes available against a given file. Return a dictionary
    allowing they consumer to look use the hash value (the key) and look up
    the hash type if needed (value).
    """
    hash_list = {}
    hashes = Hashes()
    for hash_func in hashes.checksum_functions:
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_func.update(chunk)
        hash_list[hash_func.hexdigest()] = hash_func.name
    return hash_list
