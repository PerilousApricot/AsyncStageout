#pylint: disable=C0103

"""
Spawn fake workers
"""

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from multiprocessing import Pool
import datetime, time
import logging
import traceback
import urllib
from AsyncStageOut import getHashLfn

def ftscp(user, tfc_map, config):
    """
    Fake ftscp function.
    """
    logging.debug("Work done using %s %s %s!..." %(user, tfc_map, config))
    return True


class fakeDaemon(BaseWorkerThread):
    def __init__(self, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.logger = logging.getLogger()
        
        try:
            self.config = config.AsyncTransfer
        except AttributeError:
            self.config = config.AsyncTransferTest
            
        self.logger.setLevel(self.config.log_level)
        self.logger.debug('Configuration loaded')

    def algorithm(self, parameters = None):
        """
        Give a list of users, sites and an empty tfc map
        to pool workers and object. The aim is to use the same
        mulprocessing call used in the AsyncStageOut
        """
        users = ['user1']
        sites = [u'T2_IT_Bari', u'T2_IT_Pisa']
        self.logger.debug('Active sites are : %s ' %sites)
        site_tfc_map = {}

        self.logger.debug( 'kicking off pool with size %s' %self.config.pool_size )
        pool = Pool(processes=self.config.pool_size)
        r = [pool.apply_async(ftscp, (u, site_tfc_map, self.config)) for u in users]
        pool.close()
        pool.join()

        for result in r:
            if result.ready():
                self.logger.info(result.get(1))

        return r
    
    def mark_good(self, files=[]):
        """
        Mark the list of files as tranferred
        """
        now = str(datetime.datetime.now())
        last_update = int(time.time())

        for lfn in files:

            try:
                document = self.db.document( getHashLfn(lfn) )
            except Exception, ex:
                msg =  "Error loading document from couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)

            outputLfn = document['lfn'].replace('store/temp', 'store', 1)

            try:
                data = {}
                data['end_time'] = now
                data['state'] = 'done'
                data['lfn'] = outputLfn
                data['last_update'] = last_update
                updateUri = "/" + self.db.name + "/_design/AsyncTransfer/_update/updateJobs/" + getHashLfn(lfn)
                updateUri += "?" + urllib.urlencode(data)
                self.db.makeRequest(uri = updateUri, type = "PUT", decode = False)
            except Exception, ex:
                msg =  "Error updating document in couch"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)

            outputPfn = self.apply_tfc_to_lfn( '%s:%s' % ( document['destination'], outputLfn ) )
            pluginSource = self.factory.loadObject(self.config.pluginName, args = [self.config, self.logger], listFlag = True)
            pluginSource.updateSource({ 'jobid':document['jobid'], 'timestamp':document['dbSource_update'], \
                                        'lfn': outputLfn, 'location': document['destination'], 'pfn': outputPfn, 'checksums': document['checksums'] })

        try:
            self.db.commit()
        except Exception, ex:
            msg =  "Error commiting documents in couch"
            msg += str(ex)
            msg += str(traceback.format_exc())
            self.logger.error(msg)
