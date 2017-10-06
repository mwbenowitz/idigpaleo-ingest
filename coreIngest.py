#
# This file contains the core functions for the iDigPaleo+ ingester
#

import json
import logging
import urllib
import urllib2
import feedparser
import re
import os
import shutil
import zipfile
import csv

from helpers import collectiveAccessHelpers

def createCSVSource(source, ingestTargets, test):
    logger = logging.getLogger("ingest.download")
    config = json.load(open('./config.json'))
    sourceConfig = config['sources'][source]
    testMaxRecords = config['testRecords']
    logger.debug(sourceConfig)
    importCSVs = {}
    for target in ingestTargets:
        importCSVs[target] = []
        if target not in sourceConfig['validTargets']:
            logger.error(source + "is not configured for target " + target)
            sys.exit(1)
        logger.info("Importing for target " + target)

        # Read the IPT RSS feed to get the download links
        iptURL = sourceConfig['ipt']
        logger.debug("Accessing IPT at " + iptURL)
        try:
            iptFeed = feedparser.parse(iptURL)
        except:
            logger.error("There was an issue opening the IPT at " + iptURL)

        # Get filtering criteria for this target
        collectionCriteria = sourceConfig['collectionCriteria']
        for criteria in collectionCriteria:
            if criteria['target'] != target:
                continue
            filterRegex = re.compile(criteria['filter'])
            logger.debug("Setting collection filter to: " + criteria['filter'])

            # Iterate through returned IPT collections
            for collection in iptFeed['entries']:
                logger.debug("Checking " + collection['title'] + " from the IPT")
                if filterRegex.search(collection['title']):
                    logger.info("Found matching collection: " + collection['title'])
                    nameRegex = re.compile('(Invertebrate|Vertebrate).*([0-9.]+$)')
                    collectionParse = nameRegex.search(collection['title'])
                    collectionName = collectionParse.group(1) + '_' + collectionParse.group(2)
                    if not os.path.exists(source):
                        os.makedirs(source)
                    # Download from this collection
                    collectionURL = collection['ipt_dwca']
                    logger.info("Downloading collection from " + collectionURL)

                    sourceColl = urllib2.urlopen(collectionURL).read()
                    zipFileName = source + '/' + collectionName + '.zip'
                    try:
                        logger.debug("Downloading " + zipFileName)
                        with open(zipFileName, 'wb') as zipFile:
                            zipFile.write(sourceColl)
                        # Unzip the zip file!
                        logger.debug("Unzipping " + zipFileName + " to " + collectionName)
                        collectionDir = collectionName
                        with zipfile.ZipFile(zipFileName, 'r') as unzip:
                            unzip.extractall(source + '/' + collectionDir)
                    except zipfile.BadZipfile:
                        self.logger.error("This file cannot be unzipped! Manually check for validity: " + zipFileName)
                        continue

                    logger.debug("Deleting zip file " + zipFileName)
                    os.remove(zipFileName)

                    # Open occurrence CSV file for import
                    logger.info("Opening occurrence.txt from " + source + " for import")
                    csvFile = zipFileName[:-4] + "_" + target + '_occurrence.csv'
                    csvWriter = csv.writer(open(csvFile, 'w'))
                    filterCriteria = sourceConfig['filterCriteria']
                    with open(source + '/' + collectionDir + '/occurrence.txt', 'r') as tsvFile:
                        tsvReader = csv.reader(tsvFile, delimiter="\t")
                        headerCheck = True
                        occurrenceFilters = []
                        importIdnos = []
                        testCounter = 0
                        relatedCol = None
                        for row in tsvReader:
                            if headerCheck == True:
                                logger.debug("Getting column #s of filter and others")
                                idnoCol = row.index(sourceConfig["idnoField"])
                                if sourceConfig["relatedField"]:
                                    relatedCol = row.index(sourceConfig["relatedField"])
                                # Get the columns for any defined filters
                                for criteria in filterCriteria:
                                    if criteria['target'] != target:
                                        continue
                                    try:
                                        filterCol = row.index(criteria["filterField"])
                                    except ValueError:
                                        logger.error("Source spreadsheet is missing necessary column! Check data and filters")
                                        break
                                    if criteria["filterJSON"] is True:
                                        occurrenceFilters.append({'filterCol': filterCol, 'filter': criteria['filterDict'], 'json': True})
                                    else:
                                        occurrenceFilters.append({'filterCol': filterCol, 'filter': criteria['filterValues'], 'json': False})
                                    logger.debug("Setting occurrence filter for field: " + criteria['filterField'])

                                # TODO media checking here in separate function
                                logger.debug(occurrenceFilters)
                                filterCount = len(occurrenceFilters)
                                headerCheck = False
                                csvWriter.writerow(row)
                                continue

                            #Check filters on row
                            passCounter = 0
                            for occFilter in occurrenceFilters:
                                filterValue = row[occFilter['filterCol']]
                                if not filterValue:
                                    continue
                                if occFilter['json'] is True:
                                    try:
                                        checkJSON = json.loads(filterValue)
                                    except:
                                        logger.warning("Could not read filter value in following row. SKIPPING")
                                        logger.warning(row)
                                        break
                                    if occFilter['filter']['field'] in checkJSON:
                                        checkFilter = [ val for val in occFilter['filter']['values'] if val in checkJSON[occFilter['filter']['field']] ]
                                        if checkFilter:
                                            passCounter += 1
                                else:
                                    if filterValue.lower() in occFilter['filter']:
                                        passCounter += 1

                            if passCounter == filterCount:
                                if relatedCol is not None:
                                    relatedRegex = re.compile(sourceConfig['relatedFieldRegex'])
                                    parsedRelated = relatedRegex.sub(sourceConfig['relatedFieldReplace'], row[relatedCol])
                                    row[relatedCol] = parsedRelated

                                if test:
                                    if testCounter >= testMaxRecords:
                                        break
                                    testCounter += 1
                                importIdnos.append(row[idnoCol])
                                csvWriter.writerow(row)

                    importCSVs[target].append(csvFile)

                    # Open media CSV file for import
                    mediaTXT = source + '/' + collectionDir + '/multimedia.txt'
                    if os.path.isfile(mediaTXT):
                        logger.info("Opening multimedia.txt from " + source + " for import")
                        csvMediaFile = zipFileName[:-4] + "_" + target + '_media.csv'
                        csvMediaWriter = csv.writer(open(csvMediaFile, 'w'))
                        with open(mediaTXT, 'r') as tsvMediaFile:
                            tsvMediaReader = csv.reader(tsvMediaFile, delimiter="\t")
                            mediaHeaderCheck = True
                            testCounter = 0
                            relatedCol = None
                            for row in tsvMediaReader:
                                if mediaHeaderCheck == True:
                                    mediaIdnoCol = row.index(sourceConfig['mediaIdnoField'])
                                    uriCol = row.index(sourceConfig['mediaURLField'])
                                    mediaHeaderCheck = False
                                    csvMediaWriter.writerow(row)
                                    continue

                                rawIdno = row[mediaIdnoCol]
                                idnoRegex = re.compile(sourceConfig['mediaIdnoRegex'])
                                mediaIdno = idnoRegex.sub(sourceConfig['mediaIdnoReplace'], rawIdno)

                                if mediaIdno in importIdnos:
                                    try:
                                        direct_url = urllib.urlopen(row[uriCol]).geturl()
                                        row[uriCol] = direct_url
                                    except IOError, e:
                                        logger.error("Bad media file for " + mediaIdno)
                                        continue
                                    row[mediaIdnoCol] = mediaIdno
                                    csvMediaWriter.writerow(row)
                        importCSVs[target].append(csvMediaFile)
                    else:
                        logger.info("No multimedia.txt for this collection")

                    shutil.rmtree(source + '/' + collectionDir)

    return importCSVs


def importCSVFiles(source, ingestTargets, csvFiles):
    logger = logging.getLogger("ingest.import")
    logger.info("Importing data from created spreadsheets")
    config = json.load(open('./config.json'))
    mappingDirectory = config['mappingDirectory']
    sourceConfig = config['sources'][source]
    for target in ingestTargets:
        targetCSVs = csvFiles[target]
        targetMappings = sourceConfig['collectionCriteria']
        for mappings in targetMappings:
            if mappings['target'] == target:
                occurrenceMapping = mappings['dataMapping']
                mediaMapping = mappings['mediaMapping']
                break
        logger.debug("Loading mapping from: " + mappingDirectory + occurrenceMapping)
        occurrenceMappingCode = collectiveAccessHelpers.loadImportMapping(mappingDirectory + occurrenceMapping, config['supportDirectory'])
        if not occurrenceMappingCode:
            logger.error("Can't import from " + source + " Aborting import")
            return False
        mediaMappingCode = collectiveAccessHelpers.loadImportMapping(mappingDirectory + mediaMapping, config['supportDirectory'])
        if not mediaMappingCode:
            logger.warning("Can't import media for " + source + " please review your mapping and retry")
        for csvFile in targetCSVs:
            logger.info("Importing " + csvFile)
            if 'occurrence' in csvFile:
                importMappingCode = occurrenceMappingCode
            elif 'media' in csvFile:
                importMappingCode = mediaMappingCode
            importResult = collectiveAccessHelpers.importData(csvFile, importMappingCode, config['supportDirectory'])
            if importResult is True:
                logger.info("Successfully imported " + csvFile)
            else:
                logger.warning("This data could not be imported. Check the logs")

    return True
