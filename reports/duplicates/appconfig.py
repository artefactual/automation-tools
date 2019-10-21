# -*- coding: utf-8 -*-

"""Class to help bring-together application configuration for the
de-duplication work.
"""


import json
import os

from amclient import AMClient


class AppConfig:
    """Application configuration class."""

    def __init__(self):
        """Initialize class."""
        config_file = os.path.join(os.path.dirname(__file__), "config.json")
        self._load_config(config_file)

    def _load_config(self, config_file):
        """Load our configuration information."""
        with open(config_file) as json_config:
            conf = json.load(json_config)

        self.docker = True if conf.get("docker").lower() == "true" else False

        self.storage_service_user = conf.get("storage_service_user")
        self.storage_service_api_key = conf.get("storage_service_api_key")
        self.storage_service_url = conf.get("storage_service_url")
        self.accruals_transfer_source = conf.get("accruals_transfer_source")

        # Space to configure a new location in.
        self.default_space = conf.get("default_storage_space")
        self.default_path = conf.get("default_path")

        # Information about the candidate transfer.
        self.candidate_agent = conf.get("candidate_agent")
        self.candidate_location = conf.get("candidate_location")

    def get_am_client(self):
        """Return an Archivematica API client to the caller."""
        am = AMClient()
        am.ss_url = self.storage_service_url
        am.ss_user_name = self.storage_service_user
        am.ss_api_key = self.storage_service_api_key
        return am
