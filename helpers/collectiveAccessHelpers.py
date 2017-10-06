#
# These helpers interface with CollectiveAccess to perform certain
# tasks for the ingest process
#

# system modules
from subprocess import Popen, PIPE, call
import logging
import re

logger = logging.getLogger('ingest.collectiveaccess')

def loadImportMapping(mappingFile, supportDirectory):
    logger.debug("Importing file " + mappingFile)
    importCall = Popen([supportDirectory + 'bin/caUtils', 'load-import-mapping', '-f', mappingFile], stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out, err = importCall.communicate()
    if importCall.returncode != 0:
        logger.error("load-import-mapping failed with error: " + err)
        return False
    else:
        ansiEscape = re.compile(r'(\x9B|\x1B\[)[0-?]*[ -/]*[@-~]')
        cleanOut = ansiEscape.sub('', out)
        logger.debug("Successfully loaded mapping! " + cleanOut)
        importCodeMatch = re.search(r'Created mapping (.+) from', cleanOut)
        if importCodeMatch:
            importerCode = importCodeMatch.group(1)
            logger.debug("Extracted importer code " + importerCode)
            return importerCode
        else:
            logger.error("Could not find exporter code")
            return False

def importData(dataFile, mappingCode, supportDirectory):
    logger.info("Importing " + dataFile + " with " + mappingCode)
    importCall = Popen([supportDirectory + 'bin/caUtils', 'import-data', '-s', dataFile, '-m', mappingCode, '-f', 'CSVDelimited'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out, err = importCall.communicate()
    if importCall.returncode != 0:
        logger.error("import-data failed with error: " + err)
        return False
    else:
        logger.info("Successfully imported data!\n" + out)
        return True
