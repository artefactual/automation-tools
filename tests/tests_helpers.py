# -*- coding: utf-8 -*-

import os
import shutil


class TmpDir:
    """Context manager to clear and create a temporary directory and destroy it
    after usage.
    """

    def __init__(self, tmp_dir_path):
        self.tmp_dir_path = tmp_dir_path

    def __enter__(self):
        if os.path.isdir(self.tmp_dir_path):
            shutil.rmtree(self.tmp_dir_path)
        os.makedirs(self.tmp_dir_path)
        return self.tmp_dir_path

    def __exit__(self, exc_type, exc_value, traceback):
        if os.path.isdir(self.tmp_dir_path):
            shutil.rmtree(self.tmp_dir_path)
        if exc_type:
            return None
