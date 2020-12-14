"""
_SetupCMSSWPset_

Create a CMSSW PSet suitable for running a WMAgent job.

"""
from __future__ import print_function

import json
import logging
import os
import pickle
import random
import socket
import re

from PSetTweaks.PSetTweak import PSetTweak
from PSetTweaks.WMTweak import  makeJobTweak, makeOutputTweak, makeTaskTweak, resizeResources
from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig
from WMCore.Storage.TrivialFileCatalog import TrivialFileCatalog
from WMCore.WMRuntime.ScriptInterface import ScriptInterface
from WMCore.WMRuntime.Tools.Scram import isCMSSWSupported, isEnforceGUIDInFileNameSupported, Scram


def factory(module, name):
    """
    _factory_
    Use this when loading a PSet pickle
    and a specific module is not available (i.e.: FWCore modules)

    """

    class DummyClass:
        def __init__(self, module, name, *args, **kwargs):
            self.__module = module
            self.__name = name
            self.__d = dict()

        def __setitem__(self, key, value):
            self.__d[key] = value

        def __getitem__(self, item):
            return self.__d[item]

        def __call__(self, *args, **kwargs):
            pass

        def __repr__(self):
            return "{module}.{name}".format(module=self.__module, name=self.__name)

    return DummyClass


class Unpickler(pickle.Unpickler):
    def find_class(self, module, name):
        try:
            return super().find_class(module, name)
        except:
            return factory(module, name)


class SetupCMSSWPset(ScriptInterface):
    """
    _SetupCMSSWPset_

    """

    def __init__(self, crabPSet=False):
        ScriptInterface.__init__(self)
        self.crabPSet = crabPSet
        self.process = None
        self.jobBag = None
        self.logger = logging.getLogger()
        self.tweak = PSetTweak()
        self.scram = self.createScramEnv()
        self.configPickle = getattr(self.step.data.application.command, "configurationPickle", "PSet.pkl")

    def fixupGlobalTag(self):
        """
        _fixupGlobalTag_

        Make sure that the process has a GlobalTag.globaltag string.

        Requires that the configuration already has a properly configured GlobalTag object.

        """
        if hasattr(self.process, "GlobalTag"):
            if not hasattr(self.process.GlobalTag, "globaltag"):
                self.tweak.addParameter("process.GlobalTag.globaltag", "")
        return


    def fixupGlobalTagTransaction(self):
        """
        _fixupGlobalTagTransaction_

        Make sure that the process has a GlobalTag.DBParameters.transactionId string.

        Requires that the configuration already has a properly configured GlobalTag object

        (used to customize conditions access for Tier0 express processing)

        """
        if hasattr(self.process, "GlobalTag"):
            if not hasattr(self.process.GlobalTag.DBParameters, "transactionId"):
                self.tweak.addParameter("process.GlobalTag.DBParameters.transactionId", "")
        return


    def fixupFirstRun(self):
        """
        _fixupFirstRun_

        Make sure that the process has a firstRun parameter.

        """
        if not hasattr(self.process.source, "firstRun"):
            self.tweak.addParameter("process.source.firstRun", 0)
        return


    def fixupLastRun(self):
        """
        _fixupLastRun_

        Make sure that the process has a lastRun parameter.

        """
        if not hasattr(self.process.source, "lastRun"):
            self.tweak.addParameter("process.source.lastRun", 0)

        return

    def fixupLumisToProcess(self):
        """
        _fixupLumisToProcess_

        Make sure that the process has a lumisToProcess parameter.

        """
        if not hasattr(self.process.source, "lumisToProcess"):
            self.tweak.addParameter("process.source.lumisToProcess", [])

        return


    def fixupSkipEvents(self):
        """
        _fixupSkipEvents_

        Make sure that the process has a skip events parameter.

        """
        if not hasattr(self.process.source, "skipEvents"):
            self.tweak.addParameter("process.source.skipEvents", 0)

        return


    def fixupFirstEvent(self):
        """
        _fixupFirstEvent_

        Make sure that the process has a first event parameter.

        """
        if not hasattr(self.process.source, "firstEvent"):
            self.tweak.addParameter("process.source.firstEvent", 0)
        return


    def fixupMaxEvents(self):
        """
        _fixupMaxEvents_

        Make sure that the process has a max events parameter.

        """
        if not hasattr(self.process, "maxEvents") or not hasattr(self.process.maxEvents, "input"):
            self.tweak.addParameter("process.maxEvents.input", -1)

        return

    def fixupFileNames(self):
        """
        _fixupFileNames_

        Make sure that the process has a fileNames parameter.

        """
        if not hasattr(self.process.source, "fileNames"):
            self.tweak.addParameter("process.source.fileNames", [])

        return

    def fixupSecondaryFileNames(self):
        """
        _fixupSecondaryFileNames_

        Make sure that the process has a secondaryFileNames parameter.

        """
        if not hasattr(self.process.source, "secondaryFileNames"):
            self.tweak.addParameter("process.source.secondaryFileNames", [])

        return


    def fixupFirstLumi(self):
        """
        _fixupFirstLumi

        Make sure that the process has firstLuminosityBlock parameter.
        """
        if not hasattr(self.process.source, "firstLuminosityBlock"):
            self.tweak.addParameter("process.source.firstLuminosityBlock", 1)

        return

    def createScramEnv(self):
        scram = Scram(
            version=self.getCmsswVersion(),
            directory=self.step.builder.workingDir,
            architecture=self.getScramVersion(),
            initialise=self.step.application.setup.softwareEnvironment
        )
        scram.project() # creates project area
        scram.runtime() # creates runtime environment

        return scram

    def scramRun(self, cmdArgs):
        retval = self.scram(command=cmdArgs)
        if retval > 0:
            msg = "Error running scram process. Error code: %s" % (retval)
            logging.error(msg)
            raise RuntimeError(msg)
        
    def createProcess(self, scenario, funcName, funcArgs):
        """
        _createProcess_

        Create a Configuration.DataProcessing PSet.

        """

        # @TODO: Get rid of this as soon as we are sure cmssw-wm-tools can be found
        # in all CMSSW release environments.
        # procScript = "cmssw_wm_create_process.py"
        procScript = os.path.join("/cvmfs/cms.cern.ch/slc7_amd64_gcc820/cms/cmssw-wm-tools/201113/bin", "cmssw_wm_create_process.py")

        workingDir = self.stepSpace.location
        processDic = {"scenario": scenario}
        processJson = os.path.join(workingDir, "process_scenario.json")
        funcArgsJson = os.path.join(workingDir, "process_funcArgs.json")

        if funcName == "merge" or funcName == "repack":
            try:
                with open(funcArgsJson, 'wb') as f:
                    json.dump(funcArgs, f)
            except Exception as ex:
                self.logger.exception("Error writing out process funcArgs json:")
                raise
            funcArgsParam = funcArgsJson
        else:
            try:
                with open(processJson, 'wb') as f:
                    processJson = json.dump(processDic, f)
            except Exception as ex:
                self.logger.exception("Error writing out process scenario json:")
                raise
            funcArgsParam = processJson

        self.scramRun("%s --output_pkl %s --funcname %s --funcargs %s" % (
            procScript,
            os.path.join(workingDir, "PSet.pkl"),
            funcName,
            funcArgsParam))

        return

    def loadPSet(self):
        """
        _loadPSet_

        Load a PSet that was shipped with the job sandbox.
        Mock actual Pset values that depend on CMSSW, as these are
        handled externally.

        """
        psetFile = self.configPickle
        try:
            with open(psetFile, 'rb') as f:
                self.process = Unpickler(f).load()
        except ImportError as ex:
            msg = "Unable to import pset from %s:\n" % psetFile
            msg += str(ex)
            self.logger.error(msg)
            raise ex

        return

    def applyPsetTweak(self, psetTweak, skipIfSet=False, allowFailedTweaks=False):
        # @TODO: Get rid of this as soon as we are sure cmssw-wm-tools can be found
        # in all CMSSW release environments.
        # procScript = "edm_pset_tweak.py"
        procScript = os.path.join("/cvmfs/cms.cern.ch/slc7_amd64_gcc820/cms/cmssw-wm-tools/201113/bin", "edm_pset_tweak.py")
        workingDir = self.stepSpace.location
        psetTweakJson = os.path.join(workingDir, "PSetTweak.json")
        psetTweak.persist(psetTweakJson, formatting='json')

        cmd = "%s --input_pkl %s --output_pkl %s --json %s" % (
            procScript,
            self.configPickle,
            self.configPickle,
            psetTweakJson)

    def handleSeeding(self):
        """
        _handleSeeding_

        Handle Random Seed settings for the job
        """
        stepName = self.step.data._internal_name
        workingDir = self.stepSpace.location
        seeding = getattr(self.jobBag, "seeding", None)
        seedJson = os.path.join(workingDir, "reproducible_seed.json") 
        self.logger.info("Job seeding set to: %s", seeding)
            
        # @TODO: Get rid of this as soon as we are sure cmssw-wm-tools can be found
        # in all CMSSW release environments.
        # procScript = "cmssw_handle_random_seeds.py"
        procScript = os.path.join("/cvmfs/cms.cern.ch/slc7_amd64_gcc820/cms/cmssw-wm-tools/201113/bin", "cmssw_handle_random_seeds.py")

        cmd = "%s --input_pkl %s --output_pkl %s --seeding %s" % (
            procScript,
            self.configPickle,
            self.configPickle,
            seeding)

        if seeding == "ReproducibleSeeding":
            randService = self.jobBag.process.RandomNumberGeneratorService
            seedParams = {}
            for x in randService:
                parameter = "process.RandomNumberGeneratorService.%s.initialSeed" % x._internal_name
                seedParams[parameter] = x.initialSeed
            try: 
                with open(seedJson, 'wb') as f:
                    json.dump(seedParams, f)
            except Exception as ex:
                self.logger.exception("Error writing out process funcArgs json:")
                raise
            cmd += " --reproducible_json %s" % (seedJson)

        self.scramRun(cmd)
        return


    def handlePerformanceSettings(self):
        """
        _handlePerformanceSettings_

        Install the standard performance report services
        """
        # @TODO: To be implemented
        # procScript = "cmssw_handle_performance_settings.py""
        procScript = os.path.join("/cvmfs/cms.cern.ch/slc7_amd64_gcc820/cms/cmssw-wm-tools/201113/bin", "cmssw_handle_performance_settings.py")

        return

    def makeThreadsStreamsTweak(self):
        origCores = int(getattr(self.step.data.application.multicore, 'numberOfCores', 1))
        eventStreams = int(getattr(self.step.data.application.multicore, 'eventStreams', 0))
        resources = {'cores': origCores}
        resizeResources(resources)
        numCores = resources['cores']
        if numCores != origCores:
            self.logger.info(
                "Resizing a job with nStreams != nCores. Setting nStreams = nCores. This may end badly.")
            eventStreams = 0

        self.tweak.addParameter("process.options.numberOfThreads", numCores)
        self.tweak.addParameter("process.options.numberOfStreams", eventStreams)

        return

    def handleChainedProcessingTweak(self):
        """
        _handleChainedProcessing_

        In order to handle chained processing it's necessary to feed
        output of one step/task (nomenclature ambiguous) to another.
        This method creates particular mapping in a working Trivial
        File Catalog (TFC).
        """
        self.logger.info("Handling chained processing job")
        # first, create an instance of TrivialFileCatalog to override
        tfc = TrivialFileCatalog()
        # check the jobs input files
        inputFile = ("../%s/%s.root" % (self.step.data.input.inputStepName,
                                        self.step.data.input.inputOutputModule))
        tfc.addMapping("direct", inputFile, inputFile, mapping_type="lfn-to-pfn")
        tfc.addMapping("direct", inputFile, inputFile, mapping_type="pfn-to-lfn")

        self.tweak.addParameter('process.source.fileNames', [inputFile])
        self.tweak.addParameter('process.maxEvents.input', -1)

        tfcName = "override_catalog.xml"
        tfcPath = os.path.join(os.getcwd(), tfcName)
        self.logger.info("Creating override TFC and saving into '%s'", tfcPath)
        tfcStr = tfc.getXML()
        with open(tfcPath, 'w') as tfcFile:
            tfcFile.write(tfcStr)

        self.step.data.application.overrideCatalog = "trivialcatalog_file:" + tfcPath + "?protocol=direct"

        return

    def handlePileup(self):
        """
        _handlePileup_

        Handle pileup settings.
        """
        # find out local site SE name
        siteConfig = loadSiteLocalConfig()
        PhEDExNodeName = siteConfig.localStageOut["phedex-node"]
        self.logger.info("Running on site '%s', local PNN: '%s'", siteConfig.siteName, PhEDExNodeName)

        # @TODO: Get rid of this as soon as we are sure cmssw-wm-tools can be found
        # in all CMSSW release environments.
        # procScript = "cmssw_handle_pileup.py"
        procScript = os.path.join("/cvmfs/cms.cern.ch/slc7_amd64_gcc820/cms/cmssw-wm-tools/201113/bin", "cmssw_handle_pileup.py")
        workingDir = self.stepSpace.location
        jsonPileupConfig = os.path.join(workingDir, "pileupconf.json")

        cmd = "%s --input_pkl %s --output_pkl %s --pileup_dict %s" % (
            procScript,
            self.configPickle,
            self.configPickle,
            jsonPileupConfig)

        if self.jobBag.skipPileupEvents:
            randomSeed = self.job['task']
            cmd += "--skip_pileup_events --random_seed %s" %(randomSeed)
        self.scramRun(cmd)

        return

    def handleProducersNumberOfEvents(self):
        """
        _handleProducersNumberOfEvents_

        Some producer modules are initialized with a maximum number of events
        to be generated, usually based on the process.maxEvents.input attribute
        but after that is tweaked the producers number of events need to
        be fixed as well. This method takes care of that.
        """
        # @TODO Set this externally
        return


    def handleDQMFileSaver(self):
        """
        _handleDQMFileSaver_

        Harvesting jobs have the dqmFileSaver EDAnalyzer that must
        be tweaked with the dataset name in order to store it
        properly in the DQMGUI, others tweaks can be added as well
        """

        runIsComplete = getattr(self.jobBag, "runIsComplete", False)
        multiRun = getattr(self.jobBag, "multiRun", False)
        runLimits = getattr(self.jobBag, "runLimits", "")
        self.logger.info("DQMFileSaver set to multiRun: %s, runIsComplete: %s, runLimits: %s",
                         multiRun, runIsComplete, runLimits)

        # @TODO: Get rid of this as soon as we are sure cmssw-wm-tools can be found
        # in all CMSSW release environments.
        # procScript = "cmssw_handle_dqm_filesaver.py"
        procScript = os.path.join("/cvmfs/cms.cern.ch/slc7_amd64_gcc820/cms/cmssw-wm-tools/201113/bin", "cmssw_handle_dqm_filesaver.py")
        workingDir = self.stepSpace.location

        cmd = "%s --input_pkl %s --output_pkl %s" % (
            procScript,
            self.configPickle,
            self.configPickle)

        if hasattr(self.step.data.application.configuration, "pickledarguments"):
            args = pickle.loads(self.step.data.application.configuration.pickledarguments)
            datasetName = args.get('datasetName', None)
        if datasetName:
            cmd += " --datasetName %s" % (datasetName)
        if multiRun and runLimits:
            cmd += " --multiRun --runLimits %s" % (runLimits)
        if runIsComplete:
            cmd += " --runIsComplete"
        self.scramRun(cmd)

        return

    def handleLHEInput(self):
        """
        _handleLHEInput_

        Enable lazy-download for jobs reading LHE articles from CERN, such
        that these jobs can read data remotely
        """

        if getattr(self.jobBag, "lheInputFiles", False):
            self.logger.info("Enabling 'lazy-download' for lheInputFiles job")
            self._enableLazyDownload()

        return

    def handleRepackSettings(self):
        """
        _handleRepackSettings_

        Repacking small events is super inefficient reading directly from EOS.
        """
        self.logger.info("Hardcoding read/cache strategies for repack")
        self._enableLazyDownload()
        return

    def _enableLazyDownload(self):
        """
        _enableLazyDownload_

        Set things to read data remotely
        """
        # @TODO: Get rid of this as soon as we are sure cmssw-wm-tools can be found
        # in all CMSSW release environments.
        # procScript = "cmssw_enable_lazy_download.py"
        procScript = os.path.join("/cvmfs/cms.cern.ch/slc7_amd64_gcc820/cms/cmssw-wm-tools/201113/bin", "cmssw_enable_lazy_download.py")
        cmd = "%s --input_pkl %s --output_pkl %s" % (
            procScript,
            self.configPickle,
            self.configPickle)
        self.scramRun(cmd)

        return

    def handleSingleCoreOverride(self):
        """
        _handleSingleCoreOverride_

        Make sure job only uses one core and one stream in CMSSW
        """
        try:
            if int(self.step.data.application.multicore.numberOfCores) > 1:
                self.step.data.application.multicore.numberOfCores = 1
        except AttributeError:
            pass

        try:
            if int(self.step.data.application.multicore.eventStreams) > 0:
                self.step.data.application.multicore.eventStreams = 0
        except AttributeError:
            pass

        return

    def handleSpecialCERNMergeSettings(self, funcName):
        """
        _handleSpecialCERNMergeSettings_

        CERN has a 30ms latency between Meyrin and Wigner, which kills merge performance
        Enable lazy-download for fastCloning for all CMSSW_7_5 jobs (currently off)
        Enable lazy-download for all merge jobs
        """
        if self.getCmsswVersion().startswith("CMSSW_7_5") and False:
            self.logger.info("Using fastCloning/lazydownload")
            self._enableLazyDownload()
        elif funcName == "merge":
            self.logger.info("Using lazydownload")
            self._enableLazyDownload()

        return

    def handleCondorStatusService(self):
        """
        _handleCondorStatusService_

        Enable CondorStatusService for CMSSW releases that support it.
        """
        # @TODO: Get rid of this as soon as we are sure cmssw-wm-tools can be found
        # in all CMSSW release environments.
        # procScript = "cmssw_handle_condor_status_service.py"
        procScript = os.path.join("/cvmfs/cms.cern.ch/slc7_amd64_gcc820/cms/cmssw-wm-tools/201113/bin", "cmssw_handle_condor_status_service.py")
        cmd = "%s --input_pkl %s --output_pkl %s --name %s" % (
            procScript,
            self.configPickle,
            self.configPickle,
            self.step.data._internal_name)
        self.scramRun(cmd)

        return

    def handleEnforceGUIDInFileName(self, secondaryInput=None):
        """
        _handleEnforceGUIDInFileName_

        Enable enforceGUIDInFileName for CMSSW releases that support it.
        """
        # skip it for CRAB jobs
        if self.crabPSet:
            return

        if secondaryInput:
            inputSource = secondaryInput
            self.logger.info("Evaluating enforceGUIDInFileName parameter for secondary input data.")
        else:
            inputSource = self.process.source

        if hasattr(inputSource, "type_"):
            inputSourceType = inputSource.type_()
        elif hasattr(inputSource, "_TypedParameterizable__type"):
            inputSourceType = inputSource._TypedParameterizable__type
        else:
            msg = "Source type could not be determined."
            self.logger.error(msg)
            raise AttributeError(msg)

        # only enable if source is PoolSource or EmbeddedRootSource
        if inputSourceType not in ["PoolSource", "EmbeddedRootSource"]:
            self.logger.info("Not evaluating enforceGUIDInFileName parameter for process source %s",
                             inputSourceType)
            return

        # @TODO: Get rid of this as soon as we are sure cmssw-wm-tools can be found
        # in all CMSSW release environments.
        # procScript = "cmssw_enforce_guid_in_filename.py"
        procScript = os.path.join("/cvmfs/cms.cern.ch/slc7_amd64_gcc820/cms/cmssw-wm-tools/201113/bin", "cmssw_enforce_guid_in_filename.py")
        cmd = "%s --input_pkl %s --output_pkl %s --input_source %s" % (
            procScript,
            self.configPickle,
            self.configPickle,
            inputSource.type_())
        self.scramRun(cmd)

        return

    def getCmsswVersion(self):
        """
        _getCmsswVersion_

        Return a string representing the CMSSW version to be used.
        """
        if not self.crabPSet:
            return self.step.data.application.setup.cmsswVersion
        else:
            # CRAB3 needs to use an environment var to get the version
            return os.environ.get("CMSSW_VERSION", "")


    def getScramVersion(self):
        """
        _getScramVersion_

        Return a string representing the Scram version to be used.
        """
        if not self.crabPSet:
            return self.step.data.application.setup.scramArch
        else:
            # CRAB3 needs to use an environment var to get the version
            return os.environ.get("SCRAM_ARCH", "")


    def __call__(self):
        """
        _call_

        Examine the step configuration and construct a PSet from that.

        """
        self.logger.info("Executing SetupCMSSWPSet...")
        self.jobBag = self.job.getBaggage()

        scenario = getattr(self.step.data.application.configuration, "scenario", None)
        if scenario is not None and scenario != "":
            self.logger.info("Setting up job scenario/process")
            funcName = getattr(self.step.data.application.configuration, "function", None)
            if getattr(self.step.data.application.configuration, "pickledarguments", None) is not None:
                funcArgs = pickle.loads(self.step.data.application.configuration.pickledarguments)
            else:
                funcArgs = {}
            try:
                self.createProcess(scenario, funcName, funcArgs)
            except Exception as ex:
                self.logger.exception("Error creating process for Config/DataProcessing:")
                raise ex

            if funcName == "repack":
                self.handleRepackSettings()

            if funcName in ["merge", "alcaHarvesting"]:
                self.handleSingleCoreOverride()

            if socket.getfqdn().endswith("cern.ch"):
                self.handleSpecialCERNMergeSettings(funcName)

        else:
            try:
                self.loadPSet()
            except Exception as ex:
                self.logger.exception("Error loading PSet:")
                raise ex

        # Check process.source exists
        if getattr(self.process, "source", None) is None and getattr(self.process, "_Process__source", None) is None:
            msg = "Error in CMSSW PSet: process is missing attribute 'source'"
            msg += " or process.source is defined with None value."
            self.logger.error(msg)
            raise RuntimeError(msg)

        # Fixup parameters
        self.fixupGlobalTag()
        self.fixupGlobalTagTransaction()
        self.fixupFileNames()
        self.fixupSecondaryFileNames()
        self.fixupMaxEvents()
        self.fixupSkipEvents()
        self.fixupFirstEvent()
        self.fixupFirstRun()
        self.fixupLastRun()
        self.fixupLumisToProcess()
        self.fixupFirstLumi()
        self.applyPsetTweak(self.tweak, skipIfSet=True)
        # Cleanup for pset tweaks already applied
        self.tweak = PSetTweak()

        self.handleCondorStatusService()

        # In case of CRAB3, the number of threads in the PSet should not be overridden
        if not self.crabPSet:
            try:
                self.makeThreadsStreamsTweak()
            except AttributeError as ex:
                self.logger.error("Failed to override numberOfThreads: %s", str(ex))

        # Apply task level tweaks
        taskTweak = makeTaskTweak(self.step.data, self.tweak)

        # Check if chained processing is enabled
        # If not - apply the per job tweaks
        # If so - create an override TFC (like done in PA) and then modify thePSet accordingly
        if hasattr(self.step.data.input, "chainedProcessing") and self.step.data.input.chainedProcessing:
            self.handleChainedProcessing()
        else:
            jobTweak = makeJobTweak(self.job, self.tweak)

        # check for pileup settings presence, pileup support implementation
        # and if enabled, process pileup configuration / settings
        if hasattr(self.step.data, "pileup"):
            self.handlePileup()

        # Apply per output module PSet Tweaks
        cmsswStep = self.step.getTypeHelper()
        for om in cmsswStep.listOutputModules():
            mod = cmsswStep.getOutputModule(om)
            outTweak = makeOutputTweak(mod, self.job, self.tweak)

        # revlimiter for testing
        if getattr(self.step.data.application.command, "oneEventMode", False):
            self.tweak.addParameter('process.maxEvents.input', 1)

        # check for random seeds and the method of seeding which is in the job baggage
        self.handleSeeding()

        # make sure default parametersets for perf reports are installed
        # @TODO Implement from cmssw scripts
        #self.handlePerformanceSettings()

        # check for event numbers in the producers
        # @TODO Implement from cmssw scripts
        self.handleProducersNumberOfEvents()

        # fixup the dqmFileSaver
        self.handleDQMFileSaver()

        # tweak for jobs reading LHE articles from CERN
        self.handleLHEInput()

        # tweak jobs for enforceGUIDInFileName
        self.handleEnforceGUIDInFileName()

        # Check if we accept skipping bad files
        if hasattr(self.step.data.application.configuration, "skipBadFiles"):
            self.tweak.addParameter("process.source.skipBadFiles",
                bool(self.step.data.application.configuration.skipBadFiles))

        # Apply events per lumi section if available
        if hasattr(self.step.data.application.configuration, "eventsPerLumi"):
            self.tweak.addParameter("process.source.numberEventsInLuminosityBlock",
                self.step.data.application.configuration.eventsPerLumi)

        # limit run time if desired
        if hasattr(self.step.data.application.configuration, "maxSecondsUntilRampdown"):
            self.tweak.addParameter("process.maxSecondsUntilRampdown.input",
                self.step.data.application.configuration.maxSecondsUntilRampdown)

        # accept an overridden TFC from the step
        if hasattr(self.step.data.application, 'overrideCatalog'):
            self.logger.info("Found a TFC override: %s", self.step.data.application.overrideCatalog)
            self.tweak.addParameter("process.source.overrideCatalog",
                str(self.step.data.application.overrideCatalog))

        configFile = self.step.data.application.command.configuration
        workingDir = self.stepSpace.location
        try:
            self.applyPsetTweak(self.tweak)

            with open("%s/%s" % (workingDir, configFile), 'w') as handle:
                handle.write("import FWCore.ParameterSet.Config as cms\n")
                handle.write("import pickle\n")
                handle.write("with open('%s', 'rb') as handle:\n" % self.configPickle)
                handle.write("    process = pickle.load(handle)\n")
        except Exception as ex:
            self.logger.exception("Error writing out PSet:")
            raise ex

        self.logger.info("CMSSW PSet setup completed!")

        return 0
