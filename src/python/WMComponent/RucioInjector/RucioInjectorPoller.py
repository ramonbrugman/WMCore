#!/usr/bin/env python
"""
_RucioInjectorPoller_

General reminder about PhEDEx and Rucio definitions:
* CMS dataset is equivalent to a Rucio container
* CMS block is equivalent to a Rucio dataset
* CMS file is equivalent to a Rucio file
* A Rucio replica is a file, under a given scope, at a given RSE
"""
from __future__ import division

import json
import logging
import threading
import time

from Utils.MemoryCache import MemoryCache
from Utils.Timers import timeFunction
from WMCore.DAOFactory import DAOFactory
from WMCore.Services.Rucio.Rucio import Rucio, WMRucioException, RUCIO_VALID_PROJECT
from WMCore.WMException import WMException
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread


### TODO: remove this function once PhEDExInjector is out of the game
def filterDataByTier(rawData, listTiersToInject):
    """
    This function will receive data - in the same format as returned from
    the DAO - and it will pop out anything that the component is not meant
    to inject into Rucio.
    :param rawData: the large dict of location/container/block/files
    :param listTiersToInject: a list of datatiers that we want to inject
    :return: the same dictionary as in the input, but without dataset structs
             for datatiers that we do not want to be processed by this component.
    """
    if not listTiersToInject:
        # then we are actually allowed to insert anything into Rucio
        return rawData
    for location in rawData:
        for container in list(rawData[location]):
            endTier = container.rsplit('/', 1)[1]
            if endTier not in listTiersToInject:
                logging.debug("Container %s not meant to be injected by RucioInjector", container)
                rawData[location].pop(container)
    return rawData


class RucioInjectorException(WMException):
    """
    _RucioInjectorException_

    Specific RucioInjectorPoller exception handling.
    """


class RucioInjectorPoller(BaseWorkerThread):
    """
    _RucioInjectorPoller_

    Poll the DBSBuffer database and inject files as they are created.

    The logic of this component is:
      * create a rucio container (or reuse a pre-existent one)
      * create a CMS block (or reuse a pre-existent one), block gets automatically attached
      * create file/replicas, which get automatically attached to its block as well
      * now create a CMS block rule to protect this data
      * if the block has been inserted into DBS, close the block in Rucio

    In addition to that, it has logic for rucio container subscription (rule creation),
    and block rule removal. Those follow a different polling cycle though.
    """

    def __init__(self, config):
        """
        ___init___

        Initialise class members
        """
        BaseWorkerThread.__init__(self)

        self.enabled = config.RucioInjector.enabled
        # dataset rule creation has a larger polling cycle
        self.pollRules = config.RucioInjector.pollIntervalRules
        self.lastRulesExecTime = 0
        self.createBlockRules = config.RucioInjector.createBlockRules
        self.containerDiskRuleParams = config.RucioInjector.containerDiskRuleParams
        self.containerDiskRuleRSEExpr = config.RucioInjector.containerDiskRuleRSEExpr
        self.skipRulesForTiers = config.RucioInjector.skipRulesForTiers
        self.listTiersToInject = config.RucioInjector.listTiersToInject
        if config.RucioInjector.metaDIDProject not in RUCIO_VALID_PROJECT:
            msg = "Component configured with an invalid 'project' DID: %s"
            raise RucioInjectorException(msg % config.RucioInjector.metaDIDProject)
        self.metaDIDProject = dict(project=config.RucioInjector.metaDIDProject)

        # setup cache for container and blocks (containers can be much longer, make 6 days now)
        self.containersCache = MemoryCache(config.RucioInjector.cacheExpiration * 3, set())
        self.blocksCache = MemoryCache(config.RucioInjector.cacheExpiration, set())

        self.scope = getattr(config.RucioInjector, "scope", "cms")
        self.rucioAcct = config.RucioInjector.rucioAccount
        self.rucio = Rucio(acct=self.rucioAcct,
                           hostUrl=config.RucioInjector.rucioUrl,
                           authUrl=config.RucioInjector.rucioAuthUrl,
                           configDict={'logger': self.logger})

        # metadata dictionary information to be added to block/container rules
        # cannot be a python dictionary, but a JSON string instead
        self.metaData = json.dumps(dict(agentHost=config.Agent.hostName,
                                        userAgent=config.Agent.agentName))

        self.testRSEs = config.RucioInjector.RSEPostfix
        self.filesToRecover = []

        # output data placement has a different behaviour between T0 and Production agents
        if hasattr(config, "Tier0Feeder"):
            logging.info("RucioInjector running on a T0 WMAgent")
            self.isT0agent = True
        else:
            self.isT0agent = False

        if not self.listTiersToInject:
            logging.info("Component configured to inject all the data tiers")
        else:
            logging.info("Component configured to only inject data for data tiers: %s",
                         self.listTiersToInject)
        logging.info("Component configured to skip container rule creation for data tiers: %s",
                     self.skipRulesForTiers)
        logging.info("Component configured to create block rules: %s", self.createBlockRules)

    def setup(self, parameters):
        """
        _setup_

        Create DAO Factory and setup some DAO.
        """
        myThread = threading.currentThread()
        daofactory = DAOFactory(package="WMComponent.RucioInjector.Database",
                                logger=self.logger, dbinterface=myThread.dbi)

        self.getUninjected = daofactory(classname="GetUninjectedFiles")
        self.getMigrated = daofactory(classname="GetMigratedBlocks")

        self.getUnsubscribedBlocks = daofactory(classname="GetUnsubscribedBlocks")
        self.setBlockRules = daofactory(classname="SetBlocksRule")

        self.findDeletableBlocks = daofactory(classname="GetDeletableBlocks")
        self.markBlocksDeleted = daofactory(classname="MarkBlocksDeleted")
        self.getUnsubscribedDsets = daofactory(classname="GetUnsubscribedDatasets")
        self.markSubscribed = daofactory(classname="MarkDatasetSubscribed")

        daofactory = DAOFactory(package="WMComponent.DBS3Buffer",
                                logger=self.logger, dbinterface=myThread.dbi)
        self.setStatus = daofactory(classname="DBSBufferFiles.SetPhEDExStatus")
        self.setBlockClosed = daofactory(classname="SetBlockClosed")

    @timeFunction
    def algorithm(self, parameters):
        """
        _algorithm_

        Poll the database for uninjected files and inject them into Rucio.
        """
        if not self.enabled:
            logging.info("RucioInjector component is disabled in the configuration, exiting.")
            return

        logging.info("Running Rucio injector poller algorithm...")

        try:
            # files that failed to get their status updated in dbsbuffer
            self._updateLFNState(self.filesToRecover, recovery=True)

            # get dbsbuffer_file.in_phedex = 0
            uninjectedFiles = self.getUninjected.execute()

            # while we commission Rucio within WM, not all datatiers are supposed
            # to be injected by this component. Remove any data that we are not
            # meant to process!
            uninjectedFiles = filterDataByTier(uninjectedFiles, self.listTiersToInject)

            # create containers in rucio  (and update local cache)
            containersAdded = self.insertContainers(uninjectedFiles)
            if self.containersCache.isCacheExpired():
                self.containersCache.setCache(containersAdded)
            else:
                self.containersCache.addItemToCache(containersAdded)

            # create blocks. Only update the cache once a rule gets created...
            blocksAdded = self.insertBlocks(uninjectedFiles)
            if self.blocksCache.isCacheExpired():
                self.blocksCache.setCache(blocksAdded)
            else:
                self.blocksCache.addItemToCache(blocksAdded)

            # create file replicas
            self.insertReplicas(uninjectedFiles)

            # now close blocks already uploaded to DBS
            self.closeBlocks()

            if self.lastRulesExecTime + self.pollRules <= int(time.time()):
                self.insertContainerRules()
                self.insertBlockRules()
                self.deleteBlocks()
        except Exception as ex:
            msg = "Caught unexpected exception in RucioInjector. Details:\n%s" % str(ex)
            logging.exception(msg)
            raise RucioInjectorException(msg)

        return

    def insertContainers(self, uninjectedData):
        """
        This method will insert containers into Rucio, provided they cannot be found in
        the local cache.
        :param uninjectedData: same data as it's returned from the uninjectedFiles
        :return: set of containers successfully inserted into Rucio
        """
        logging.info("Preparing to insert containers into Rucio...")
        newContainers = set()
        for location in uninjectedData:
            for container in uninjectedData[location]:
                # same container can be at multiple locations
                if container not in self.containersCache and container not in newContainers:
                    if self.rucio.createContainer(container, meta=self.metaDIDProject):
                        logging.info("Container %s inserted into Rucio", container)
                        newContainers.add(container)
                    else:
                        logging.error("Failed to create container: %s", container)
        logging.info("Successfully inserted %d containers into Rucio", newContainers)
        return newContainers

    def insertBlocks(self, uninjectedData):
        """
        This method will insert blocks into Rucio and attach them to their correspondent
        containers, when attaching this block, we also need to provide the RSE that it
        will be available.
        :param uninjectedData: same data as it's returned from the uninjectedFiles
        :return: a dictionary of successfully inserted blocks and their correspondent location
        """
        logging.info("Preparing to insert blocks into Rucio...")
        newBlocks = set()
        for location in uninjectedData:
            rseName = "%s_Test" % location if self.testRSEs else location
            for container in uninjectedData[location]:
                for block in uninjectedData[location][container]:
                    if block not in self.blocksCache:
                        if self.rucio.createBlock(block, rse=rseName, meta=self.metaDIDProject):
                            logging.info("Block %s inserted into Rucio", block)
                            newBlocks.add(block)
                        else:
                            logging.error("Failed to create block: %s", block)
        logging.info("Successfully inserted %d blocks into Rucio", newBlocks)
        return newBlocks

    # TODO: this will likely go away once the phedex to rucio migration is over
    def _isBlockTierAllowed(self, blockName):
        """
        Checks whether this blockname contains a datatier that we want to
        be handled by this component, thus block-level rule creation as well
        :return: True if the component can proceed with this block, False otherwise
        """
        if not self.listTiersToInject:
            return True
        endBlock = blockName.rsplit('/', 1)[1]
        endTier = endBlock.split('#')[0]
        if endTier not in self.listTiersToInject:
            return False
        return True

    def insertBlockRules(self):
        """
        Creates a simple replication rule for every single block that
        is under production in a given site/RSE.
        Also persist the rule ID in the database.
        """
        if not self.createBlockRules:
            return

        logging.info("Preparing to create block rules into Rucio...")

        unsubBlocks = self.getUnsubscribedBlocks.execute()

        for item in unsubBlocks:
            if not self._isBlockTierAllowed(item['blockname']):
                logging.debug("Component configured to skip block rule for: %s", item['blockname'])
                continue
            # first, check if the block has already been created in Rucio
            if not self.rucio.didExist(item['blockname']):
                logging.warning("Block: %s not yet in Rucio. Retrying later..", item['blockname'])
                continue
            kwargs = dict(activity="Production Output", account=self.rucioAcct,
                          grouping="DATASET", comment="WMAgent automatic container rule",
                          ignore_availability=True, meta=self.metaData)
            rseName = "%s_Test" % item['pnn'] if self.testRSEs else item['pnn']
            # DATASET = replicates all files in the same block to the same RSE
            resp = self.rucio.createReplicationRule(item['blockname'],
                                                    rseExpression=rseName, **kwargs)
            if resp:
                msg = "Block rule created for block: %s, at: %s, with rule id: %s"
                logging.info(msg, item['blockname'], item['pnn'], resp[0])
                binds = {'RULE_ID': resp[0], 'BLOCKNAME': item['blockname']}
                self.setBlockRules.execute(binds)
            else:
                logging.error("Failed to create rule for block: %s at %s", item['blockname'], rseName)
        return

    def insertReplicas(self, uninjectedData):
        """
        Inserts replicas into Rucio and attach them to its specific block.
        If the insertion succeeds, also switch their database state to injected.

        :param uninjectedData: dictionary with blocks as key, and RSEs as value
        """
        # FIXME: I think we need a different data struct from the database
        # this method is very expensive O(n^4)
        logging.info("Preparing to insert replicas into Rucio...")

        for location in uninjectedData.keys():
            rseName = "%s_Test" % location if self.testRSEs else location
            for container in uninjectedData[location]:
                for block in uninjectedData[location][container]:
                    if block not in self.blocksCache:
                        logging.warning("Skipping %d file injection for block that failed to be added into Rucio: %s",
                                        len(uninjectedData[location][container][block]['files']), block)
                        continue
                    injectData = []
                    listLfns = []
                    for fileInfo in uninjectedData[location][container][block]['files']:
                        listLfns.append(fileInfo['lfn'])
                        injectData.append(dict(name=fileInfo['lfn'], scope=self.scope,
                                               bytes=fileInfo['size'], state="A",
                                               adler32=fileInfo['checksum']['adler32']))

                    if self.rucio.createReplicas(rse=rseName, files=injectData, block=block):
                        logging.info("Successfully inserted %d files on block %s", len(listLfns), block)
                        self._updateLFNState(listLfns)
        return

    def _updateLFNState(self, listLfns, recovery=False):
        """
        Given a list of LFNs, update their state in dbsbuffer table.
        :param listLfns: list of LFNs
        :param recovery: True if we are recovering previously injected files
        :return: nothing
        """
        if not listLfns:
            return
        try:
            self.setStatus.execute(listLfns, 1)
        except Exception as ex:
            # save it to try to inject them again in the next cycle
            self.filesToRecover.extend(listLfns)
            if 'Deadlock found' in str(ex) or 'deadlock detected' in str(ex):
                logging.error("Deadlock during file status update. Retrying again in the next cycle.")
                self.filesToRecover.extend(listLfns)
            else:
                msg = "Failed to update file status in the database, reason: %s" % str(ex)
                logging.error(msg)
                raise RucioInjectorException(msg)
        else:
            if recovery:
                self.filesToRecover = []

    def closeBlocks(self):
        """
        Close any blocks that have been migrated to global DBS
        """
        logging.info("Starting closeBlocks method")

        # in short, dbsbuffer_file.in_phedex = 1 AND dbsbuffer_block.status = 'InDBS'
        migratedBlocks = self.getMigrated.execute()
        ### FIXME the data format returned by this DAO
        for location in migratedBlocks:
            for container in migratedBlocks[location]:
                if not self._isContainerTierAllowed(container, checkRulesList=False):
                    continue
                for block in migratedBlocks[location][container]:
                    logging.info("Closing block: %s", block)
                    if self.rucio.closeBlockContainer(block):
                        self.setBlockClosed.execute(block)
                    else:
                        logging.error("Failed to close block: %s. Will retry again later.", block)

    def deleteBlocks(self):
        """
        _deleteBlocks_
        Find deletable blocks, then decide if to delete based on:
        Has transfer to all destinations finished ?
          If yes => Delete rules associated with the block, set deleted=1
          If no => do nothing (check again next cycle)
        """
        logging.info("Checking if there are block rules to be deleted...")

        # Get list of blocks that can be deleted
        blockDict = self.findDeletableBlocks.execute(transaction=False)

        if not blockDict:
            logging.info("No candidate blocks found for rule deletion")
            return

        logging.info("Found %d candidate blocks for rule deletion", len(blockDict))

        blocksToDelete = []
        containerDict = {}
        # Populate containerDict, assigning each block to its correspondant container
        for blockName in blockDict:
            container = blockDict[blockName]['dataset']
            # If the container is not in the dictionary, create a new entry for it
            if container not in containerDict:
                # Set of sites to which the container needs to be transferred
                sites = set(x.replace("_MSS", "_Tape") for x in blockDict[blockName]['sites'])
                containerDict[container] = {'blocks': [], 'rse': sites}
            containerDict[container]['blocks'].append(blockName)

        for contName in containerDict:
            cont = containerDict[contName]

            # Checks if the container is not requested in any sites.
            # This should never be triggered, but better safe than sorry
            if not cont['rse']:
                logging.warning("No rules for container: %s. Its blocks won't be deleted.", contName)
                continue

            try:
                # Get RSE in which each block is available
                availableRSEs = self.rucio.getReplicaInfoForBlocks(block=cont['blocks'])
            except Exception as exc:
                msg = "Failed to get replica info for blocks in container: %s.\n" % contName
                msg += "Will retry again in the next cycle. Error: %s" % str(exc)
                logging.error(msg)
                continue

            for blockRSEs in availableRSEs:
                # If block is available at every RSE its container needs to be transferred, the block can be deleted
                blockSites = set(blockRSEs['replica'])
                if cont['rse'].issubset(blockSites):
                    blocksToDelete.append(blockRSEs['name'])

        # Delete agent created rules locking the block
        binds = []
        logging.info("Going to delete %d block rules", len(blocksToDelete))
        for block in blocksToDelete:
            try:
                rules = self.rucio.listDataRules(block, scope=self.scope, account=self.rucioAcct)
            except WMRucioException as exc:
                logging.warning("Unable to retrieve replication rules for block: %s. Will retry in the next cycle.", block)
            else:
                if not rules:
                    logging.info("Block rule for: %s has been deleted by previous cycles", block)
                    binds.append({'DELETED': 1, 'BLOCKNAME': block})
                    continue
                for rule in rules:
                    deletedRules = 0
                    if self.rucio.deleteRule(rule['id'], purgeReplicas=True):
                        logging.info("Successfully deleted rule: %s, for block %s.", rule['id'], block)
                        deletedRules += 1
                    else:
                        logging.warning("Failed to delete rule: %s, for block %s. Will retry in the next cycle.", rule['id'], block)
                if deletedRules == len(rules):
                    binds.append({'DELETED': 1, 'BLOCKNAME': block})
                    logging.info("Successfully deleted all rules for block %s.", block)


        self.markBlocksDeleted.execute(binds)
        logging.info("Marked %d blocks as deleted in the database", len(binds))
        return

    # TODO: this will likely go away once the phedex to rucio migration is over
    def _isContainerTierAllowed(self, containerName, checkRulesList=True):
        """
        It compares the container datatier name to check whether the component
        should inject data for it or not.
        In addition to that, it can also evaluate whether it's allowed to create
        rules for such datatier or not.
        :param containerName: string with the name of the container
        :param checkRulesList: boolean to check or not against the list of tiers
          to be skipped in the rule creation
        :return: True if the component can proceed with this container, False otherwise
        """
        endTier = containerName.rsplit('/', 1)[1]
        if self.listTiersToInject and endTier not in self.listTiersToInject:
            return False
        if checkRulesList and endTier in self.skipRulesForTiers:
            return False
        return True

    def insertContainerRules(self):
        """
        Polls the database for containers meant to be subscribed and create
        a container level rule to replicate all the files to a given RSE.
        It deals with both Central Production and T0 data rules, which require
        a different approach, such as:
          * Production Tape/Custodial data placement is skipped and data is marked as transferred
          * Production Disk/NonCutodial has a generic RSE expression and some rules override
            from the agent configuration (like number of copies, grouping and weight)
          * T0 Tape is created as defined, with a special rule activity for Tape
          * T0 Disk is created as defined, with a special rule activity for Disk/Export
        """
        logging.info("Starting insertContainerRules method")

        ruleComment = "WMAgent automatic container rule"
        if self.isT0agent:
            ruleComment = "T0 " + ruleComment

        # FIXME also adapt the format returned by this DAO
        # Check for completely unsubscribed datasets that are already marked as in_phedex = 1
        unsubscribedDatasets = self.getUnsubscribedDsets.execute()

        # Keep a list of subscriptions to tick as subscribed in the database
        subscriptionsMade = []

        # Create the subscription objects and add them to the list
        # The list takes care of the sorting internally
        for subInfo in unsubscribedDatasets:
            rseName = subInfo['site'].replace("_MSS", "_Tape")
            container = subInfo['path']
            # Skip central production Tape rules
            if not self.isT0agent and rseName.endswith("_Tape"):
                logging.info("Bypassing Production container Tape data placement for container: %s and RSE: %s",
                             container, rseName)
                subscriptionsMade.append(subInfo['id'])
                continue
            # then check if the container has already been created in Rucio
            if not self.rucio.didExist(container):
                logging.warning("Container: %s not yet in Rucio. Retrying later..", container)
                continue

            ruleKwargs = dict(ask_approval=False,
                              activity=self._activityMap(rseName),
                              account=self.rucioAcct,
                              grouping="ALL",
                              comment=ruleComment,
                              meta=self.metaData)
            if not rseName.endswith("_Tape"):
                # add extra parameters to the Disk rule as defined in the component configuration
                ruleKwargs.update(self.containerDiskRuleParams)

            if not self.isT0agent:
                # destination for production Disk rules are always overwritten
                rseName = self.containerDiskRuleRSEExpr
                if self.testRSEs:
                    rseName = rseName.replace("cms_type=real", "cms_type=test")
            else:
                # then it's a T0 container placement
                if self.testRSEs:
                    rseName = "%s_Test" % rseName
                #Checking whether we need to ask for rule approval
                try:
                    if self.rucio.requiresApproval(rseName):
                        ruleKwargs['ask_approval'] = True
                except WMRucioException as exc:
                    msg = str(exc)
                    msg += "\nUnable to check approval requirements. Will retry again in the next cycle."
                    logging.error(msg)
                    continue

            logging.info("Creating container rule for %s against RSE %s", container, rseName)
            logging.debug("Container rule will be created with keyword args: %s", ruleKwargs)
            try:
                resp = self.rucio.createReplicationRule(container,
                                                        rseExpression=rseName, **ruleKwargs)
            except Exception:
                msg = "Failed to create container rule for (retrying with approval): %s" % container
                logging.warning(msg)
                ruleKwargs["ask_approval"] = True
                try:
                    resp = self.rucio.createReplicationRule(container,
                                                            rseExpression=rseName, **ruleKwargs)
                except Exception as exc:
                    msg = "Failed once again to create container rule for: %s " % container
                    msg += "\nWill retry again in the next cycle. Error: %s" % str(exc)
                    continue
            if resp:
                logging.info("Container rule created for %s under rule id: %s", container, resp)
                subscriptionsMade.append(subInfo['id'])
            else:
                logging.error("Failed to create rule for container: %s", container)

        # Register the result in DBSBuffer
        if subscriptionsMade:
            self.markSubscribed.execute(subscriptionsMade)
            logging.info("%d containers successfully locked in Rucio and local database", len(subscriptionsMade))

        return

    def _activityMap(self, rseName):
        """
        It maps the WMAgent type (Production vs T0) and the RSE name to
        properly set the rule activity field
        :param rseName: a string with the RSE name
        :return: a string with the rule activity
        """
        if not self.isT0agent and not rseName.endswith("_Tape"):
            return "Production Output"
        elif self.isT0agent and rseName.endswith("_Tape"):
            return "T0 Tape"
        elif self.isT0agent:
            return "T0 Export"
        else:
            msg = "This code should never be reached. Report it to the developers. "
            msg += "Trying to create container rule for RSE name: {}".format(rseName)
            raise WMRucioException(msg)
