#
# Helpers for iDigPaleo+ ingest process
#
import os
import shutil
from importlib import import_module
import argparse
import logging
import json
import hashlib
import pandas as pd
from tempfile import NamedTemporaryFile
import csv

def createParser():
    parser = argparse.ArgumentParser(description="Import data from IPTs into iDigPaleo + associated projects")
    parser.add_argument('-A', '--ALL', help="Import from all sources to all projects defined in your config.json file")
    parser.add_argument('-s', '--sources', nargs='+', help='REQUIRED. A list of institutions to import data from')
    parser.add_argument('-p', '--projects', nargs='+', help="Target project to import these records to")
    parser.add_argument('-t', '--test', action="store_true", help="Import only a small set of test records from search source institution")
    parser.add_argument('-l', '--logLevel', help="Set the level of message to be logged. Options: DEBUG|INFO|WARNING|ERROR")
    parser.add_argument('-li', '--listInstitutions', action="store_true", help="List institutions with available importers and the projects they can contribute to")
    return parser
