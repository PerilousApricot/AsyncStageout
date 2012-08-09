'''
Created by Andrew Melo <andrew.melo@gmail.com> on Aug 8, 2012

'''
from AsyncStageOut.TransferWorker import TransferWorker
from WMCore.Database.CMSCouch import CouchServer
from WMCore.WMFactory import WMFactory

import random, logging

# I think I want this as a module-level global to allow external
# users (i.e. unittests) to properly change things

failProbability = 0.0
def setFailProbability( value ):
    global failProbability
    failProbability = value

def getFailProbability():
    global failProbability
    return failProbability

class FakeTransferWorker(TransferWorker):
    '''
    When monkeypatched into TransferDaemon, this will replace the "normal" TransferWorker
    and allow for unittests that don't actually call FTS
    
    this should be the minimal things to fake out running FTS
    '''


    def __init__(self, user, tfc_map, config):
        """
        store the user and tfc the worker
        """
        self.user = user[0]
        self.group = user[1]
        self.role = user[2]
        self.tfc_map = tfc_map
        self.config = config    
        server = CouchServer(self.config.couch_instance)
        self.db = server.connectDatabase(self.config.files_database)
        logging.basicConfig(level=config.log_level)
        self.logger = logging.getLogger('AsyncTransfer-Worker-%s' % self.user)
        query = {'group': True,
                 'startkey':[self.user], 'endkey':[self.user, {}, {}]}
        self.userDN = self.db.loadView('AsyncTransfer', 'ftscp', query)['rows'][0]['key'][5]
        self.pfn_to_lfn_mapping = {}
        self.max_retry = config.max_retry
        # Set up a factory for loading plugins
        self.factory = WMFactory(self.config.pluginDir, namespace = self.config.pluginDir)

    def command(self):
        """
        For each job the worker has to complete:
           Delete files that have failed previously
           Create a temporary copyjob file
           Submit the copyjob to the appropriate FTS server
           Parse the output of the FTS transfer and return complete and failed files for recording
        """
        self.logger.info("beginning command")
        jobs = self.files_for_transfer()
        transferred_files = []
        failed_files = []

        failed_to_clean = {}

        #Loop through all the jobs for the links we have
        for link, copyjob in jobs.items():
            self.logger.info("checking copyjob")

            if not self.validate_copyjob( copyjob ): continue
            self.logger.info("copyjob ok")

            for task in copyjob:
                lfn = '/store' + task.split(' ')[0].split('/store')[1]
                if (random.random() < getFailProbability()):
                    failed_files.append( lfn )
                else:
                    transferred_files.append( lfn )

        return transferred_files, failed_files, []