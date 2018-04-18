#!/usr/bin/env python
#
# Script to Ingest records to iDigPaleo, Cretaceous World, and potentially other
# sub-sites/sub-projects
#

#
# by Michael Benowitz
# Whirl-i-Gig Inc.
#

# core dependencies
import sys
import os
import json
import datetime
import argparse
import logging

# import/ingest dependencies
import urllib2
import requests
from unidecode import unidecode

# Local Modules
import coreIngest

# Helper functions for managing ingest
from helpers import ingestHelpers
from helpers import logHelpers
from helpers import collectiveAccessHelpers

def main():
    os.environ["COLLECTIVEACCESS_HOME"] = "/data/idigpaleo/admin"
    config = json.load(open('./config.json'))
    # Get the parameters provided in the args
    parser = ingestHelpers.createParser()
    args = parser.parse_args()
    ingestAll = args.ALL
    ingestSources = args.sources
    ingestTargets = args.projects
    testRun = args.test
    logLevel = args.logLevel

    # Create the log
    logger = logHelpers.createLog('ingest', logLevel)
    logger.info("Starting " + str(ingestTargets) + " ingest")

    # If we just want a list of instutions and targets, do that
    if args.listInstitutions:
        sources = config['sources']
        for source in sources:
            sys.stdout.write(source + "\nPossible Target Projects: ")
            for project in sources[source]["validTargets"]:
                sys.stdout.write(project + " ")
            sys.stdout.write("\n")
        return True

    # If we don't have any sources or targets throw errors
    if ingestSources == None or ingestTargets == None and ingestAll == False:
        logger.error("You must define at least one source and one target")
        sys.exit(0)

    # If the --ALL flag got set, disregard anything else and just do a full import
    if ingestAll == True:
        logger.info("Importing from all sources for all targets")
        ingestTargets = config['targetProjects']
        ingestSources = []
        for source in config['sources']:
            ingestSources.append(source)

    # Start the relevant imports
    for source in ingestSources:
        logger.info("Starting ingest for " + source)
        importCSVs = coreIngest.createCSVSource(source, ingestTargets, testRun)
        logger.debug(importCSVs)

        importResults = coreIngest.importCSVFiles(source, ingestTargets, importCSVs)
        if importResults is True:
            logger.info("Successfully completed import. Check logs for partial import failures or skipped datasets")
        else:
            logger.error("Import was not completed!")


if __name__ == '__main__':
    main()
