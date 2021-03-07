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
from WMCore.WMRuntime.Tools.Scram import Scram

FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
logging.basicConfig(format=FORMAT)

def factory(module, name):
    """
    _factory_
    Function to return a dummy module name when a module
    is not available

    """

    class DummyClass:
        def __init__(self, module, name='', *args, **kwargs):
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
    """
    _Unpickler_
    Use this when loading a PSet pickle
    and a specific module is not available (i.e.: FWCore modules)

    """
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
        self.scram = None
        self.configPickle = "Pset.pkl"
        self.psetFile = None

    def createScramEnv(self):
        scram = Scram(
            version=self.getCmsswVersion(),
            directory=self.stepSpace.location,
            architecture=self.getScramVersion(),
            initialise=self.step.data.application.setup.softwareEnvironment
        )
        scram.project() # creates project area
        scram.runtime() # creates runtime environment

        return scram

    def scramRun(self, cmdArgs):
        if self.scram:
            retval = self.scram(command=cmdArgs)
            if retval > 0:
                msg = "Error running scram process. Error code: %s" % (retval)
                logging.error(msg)
                raise RuntimeError(msg)
        else:
            raise RuntimeError("Scram is not defined")
        
    def createProcess(self, scenario, funcName, funcArgs):
        """
        _createProcess_

        Create a Configuration.DataProcessing PSet.

        """

        procScript = "cmssw_wm_create_process.py"

        processDic = {"scenario": scenario}
        processJson = os.path.join(self.stepSpace.location, "process_scenario.json")
        funcArgsJson = os.path.join(self.stepSpace.location, "process_funcArgs.json")

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

        cmd = "%s --output_pkl %s --funcname %s --funcargs %s" % (
            procScript,
            os.path.join(self.stepSpace.location, self.configPickle),
            funcName,
            funcArgsParam)

        if funcName == "merge":
            self.logger.info("useErrorDataset = {0}".format(getattr(self.jobBag, "useErrorDataset", False)))
            if getattr(self.jobBag, "useErrorDataset", False):
                cmd += " --useErrorDataset"

        self.scramRun(cmd)
        return

    def loadPSet(self):
        """
        _loadPSet_

        Load a PSet that was shipped with the job sandbox.
        Mock actual Pset values that depend on CMSSW, as these are
        handled externally.

        """
        self.logger.info("Working dir: %s" % os.getcwd())
        # Pickle original pset configuration
        procScript = "edm_pset_pickler.py"
        pset = os.path.join(self.stepSpace.location, self.psetFile)
        cmd = "%s --input %s --output_pkl %s" % (
            procScript,
            os.path.join(self.stepSpace.location, self.psetFile),
            os.path.join(self.stepSpace.location, self.configPickle))
        self.scramRun(cmd)

        try:
            with open(os.path.join(self.stepSpace.location, self.configPickle), 'rb') as f:
                self.process = Unpickler(f).load()
        except ImportError as ex:
            msg = "Unable to import pset from %s:\n" % psetFile
            msg += str(ex)
            self.logger.error(msg)
            raise ex

        return

    def fixupProcess(self):
        """
        _fixupProcess_
        Look over the process object and make sure that all of the attributes
        that we expect to exist actually exist.
        """
        # Make sure that for each output module the following parameters exist
        # in the PSet returned from the framework:
        #   fileName
        #   logicalFileName
        #   dataset.dataTier
        #   dataset.filterName
        if hasattr(self.process, "outputModules"):
            outputModuleNames = self.process.outputModules.keys()
        elif hasattr(self.process, "outputModules_"):
            outputModuleNames = self.process.outputModules_()
        elif hasattr(self.process, "_Process__outputmodules"):
            outputModuleNames = self.process._Process__outputmodules.keys()
        else:
            msg = "Error loading output modules from process"
            raise AttributeError(msg)

        for outMod in outputModuleNames:
            tweak = PSetTweak()
            self.logger.info("DEBUG output module = %s" % outMod)
            tweak.addParameter("process.options", "customTypeCms.untracked.PSet()")
            tweak.addParameter("process.%s.dataset" % outMod, "customTypeCms.untracked.PSet(dataTier=cms.untracked.string(''), filterName=cms.untracked.string(''))")
            self.applyPsetTweak(tweak, skipIfSet=True, cleanupTweak=True)
            #tweak.addParameter("process.%s.dataset.dataTier" % outMod, "customTypeCms.untracked.string('')")
            #tweak.addParameter("process.%s.dataset.filterName" % outMod, "customTypeCms.untracked.string('')")
            tweak.addParameter("process.%s.fileName" % outMod, "customTypeCms.untracked.string('')")
            tweak.addParameter("process.%s.logicalFileName" % outMod, "customTypeCms.untracked.string('')")
            self.applyPsetTweak(tweak, skipIfSet=True)

        return

    def applyPsetTweak(self, psetTweak, skipIfSet=False, allowFailedTweaks=False, name='', cleanupTweak=False):
        procScript = "edm_pset_tweak.py"
        psetTweakJson = os.path.join(self.stepSpace.location, "PSetTweak%s.json" % name)
        psetTweak.persist(psetTweakJson, formatting='simplejson')

        cmd = "%s --input_pkl %s --output_pkl %s --json %s" % (
            procScript,
            os.path.join(self.stepSpace.location, self.configPickle),
            os.path.join(self.stepSpace.location, self.configPickle),
            psetTweakJson)
        if skipIfSet:
            cmd += " --skip_if_set"
        if allowFailedTweaks:
            cmd += " --allow_failed_tweaks"
        self.scramRun(cmd)

        if cleanupTweak is True:
            psetTweak = PSetTweak()

        return

    def handleSeeding(self):
        """
        _handleSeeding_

        Handle Random Seed settings for the job
        """
        stepName = self.step.data._internal_name
        seeding = getattr(self.jobBag, "seeding", None)
        seedJson = os.path.join(self.stepSpace.location, "reproducible_seed.json") 
        self.logger.info("Job seeding set to: %s", seeding)
        procScript = "cmssw_handle_random_seeds.py"

        cmd = "%s --input_pkl %s --output_pkl %s --seeding %s" % (
            procScript,
            os.path.join(self.stepSpace.location, self.configPickle),
            os.path.join(self.stepSpace.location, self.configPickle),
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
        tweak = PSetTweak()
        # include the default performance report services
        if getattr(self.step.data.application.command, 'silentMemoryCheck', False):
            tweak.addParameter("process.SimpleMemoryCheck", "customTypeCms.Service('SimpleMemoryCheck', jobReportOutputOnly=cms.untracked.bool(True))")
        else:
            tweak.addParameter("process.SimpleMemoryCheck", "customTypeCms.Service('SimpleMemoryCheck')")

        tweak.addParameter("process.CPU", "customTypeCms.Service('CPU')")
        tweak.addParameter("process.Timing", "customTypeCms.Service('Timing')")
        self.applyPsetTweak(tweak)
        self.tweak.addParameter("process.Timing.summaryOnly", "customTypeCms.untracked(cms.bool(True))")

        return

    def makeThreadsStreamsTweak(self):
        """
        _makeThreadsStreamsTweak_

        Tweak threads and streams paraameters
        """
        origCores = int(getattr(self.step.data.application.multicore, 'numberOfCores', 1))
        eventStreams = int(getattr(self.step.data.application.multicore, 'eventStreams', 0))
        resources = {'cores': origCores}
        resizeResources(resources)
        numCores = resources['cores']
        if numCores != origCores:
            self.logger.info(
                "Resizing a job with nStreams != nCores. Setting nStreams = nCores. This may end badly.")
            eventStreams = 0

        tweak = PSetTweak()
        tweak.addParameter("process.options", "customTypeCms.untracked.PSet()")
        self.applyPsetTweak(tweak, skipIfSet=True)
        self.tweak.addParameter("process.options.numberOfThreads", "customTypeCms.untracked.uint32(%s)" % numCores)
        self.tweak.addParameter("process.options.numberOfStreams", "customTypeCms.untracked.uint32(%s)" % eventStreams)

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

        self.tweak.addParameter('process.source.fileNames', "customTypeCms.untracked.vstring(%s)" % [inputFile])
        self.tweak.addParameter("process.maxEvents", "customTypeCms.untracked.PSet(input=cms.untracked.int32(-1))")

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
        jsonPileupConfig = os.path.join(self.stepSpace.location, "pileupconf.json")

        # Load pileup json 
        try:
            with open(jsonPileupConfig) as jdata:
                pileupDict = json.load(jdata)
        except IOError:
            m = "Could not read pileup JSON configuration file: '%s'" % jsonPileupConfig
            raise RuntimeError(m)

        # Create a json with a list of files and events available 
        # after dealing with PhEDEx/AAA logic
        newPileupDict = {}
        fileList = []
        eventsAvailable = 0
        for pileupType in self.step.data.pileup.listSections_():
            useAAA = True if getattr(self.jobBag, 'trustPUSitelists', False) else False
            self.logger.info("Pileup set to read data remotely: %s", useAAA)
            for blockName in sorted(pileupDict[pileupType].keys()):
                blockDict = pileupDict[pileupType][blockName]
                if PhEDExNodeName in blockDict["PhEDExNodeNames"] or useAAA:
                    eventsAvailable += int(blockDict.get('NumberOfEvents', 0))
                    for fileLFN in blockDict["FileList"]:
                        fileList.append(str(fileLFN))
            newPileupDict[pileupType] = {"eventsAvailable": eventsAvailable, "FileList": fileList}
        newJsonPileupConfig = os.path.join(self.stepSpace.location, "CMSSWPileupConfig.json")
        self.logger.info("Generating json for CMSSW pileup script")
        try:
            with open(newJsonPileupConfig, 'wb') as f:
                json.dump(newPileupDict, f)
        except Exception as ex:
            self.logger.exception("Error writing out process filelist json:")
            raise

        procScript = "cmssw_handle_pileup.py"
        cmd = "%s --input_pkl %s --output_pkl %s --pileup_dict %s" % (
            procScript,
            os.path.join(self.stepSpace.location, self.configPickle),
            os.path.join(self.stepSpace.location, self.configPickle),
            newJsonPileupConfig)

        if getattr(self.jobBag, "skipPileupEvents", None):
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

        procScript = "cmssw_handle_nEvents.py"
        cmd = "%s --input_pkl %s --output_pkl %s" % (
            procScript,
            os.path.join(self.stepSpace.location, self.configPickle),
            os.path.join(self.stepSpace.location, self.configPickle))
        self.scramRun(cmd)

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

        procScript = "cmssw_handle_dqm_filesaver.py"

        cmd = "%s --input_pkl %s --output_pkl %s" % (
            procScript,
            os.path.join(self.stepSpace.location, self.configPickle),
            os.path.join(self.stepSpace.location, self.configPickle))

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
        procScript = "cmssw_enable_lazy_download.py"
        cmd = "%s --input_pkl %s --output_pkl %s" % (
            procScript,
            os.path.join(self.stepSpace.location, self.configPickle),
            os.path.join(self.stepSpace.location, self.configPickle))
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
        procScript = "cmssw_handle_condor_status_service.py"
        cmd = "%s --input_pkl %s --output_pkl %s --name %s" % (
            procScript,
            os.path.join(self.stepSpace.location, self.configPickle),
            os.path.join(self.stepSpace.location, self.configPickle),
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

        procScript = "cmssw_enforce_guid_in_filename.py"
        cmd = "%s --input_pkl %s --output_pkl %s --input_source %s" % (
            procScript,
            os.path.join(self.stepSpace.location, self.configPickle),
            os.path.join(self.stepSpace.location, self.configPickle),
            inputSourceType)
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
        self.logger.info('jobBag = {0}'.format(dir(self.jobBag)))
        self.logger.info('jobBag useErrorDataset = {0}'.format(getattr(self.jobBag, "useErrorDataset", "Unset")))
        self.configPickle = getattr(self.step.data.application.command, "configurationPickle", "PSet.pkl")
        self.psetFile = getattr(self.step.data.application.command, "configuration", "PSet.py")
        self.scram = self.createScramEnv()

        scenario = getattr(self.step.data.application.configuration, "scenario", None)
        if scenario is not None and scenario != "":
            self.logger.info("DEBUG: I'm in scenario")
            self.logger.info("Setting up job scenario/process")
            funcName = getattr(self.step.data.application.configuration, "function", None)
            if getattr(self.step.data.application.configuration, "pickledarguments", None) is not None:
                funcArgs = pickle.loads(self.step.data.application.configuration.pickledarguments)
            else:
                funcArgs = {}

            # Create process
            try:
                self.createProcess(scenario, funcName, funcArgs)
            except Exception as ex:
                self.logger.exception("Error creating process for Config/DataProcessing:")
                raise ex
            # Now, load the new picked process 
            try:
                with open(os.path.join(self.stepSpace.location, self.configPickle), 'rb') as f:
                    self.process = Unpickler(f).load()
            except ImportError as ex:
                msg = "Unable to import pset from %s:\n" % psetFile
                msg += str(ex)
                self.logger.error(msg)
                raise ex

            if funcName == "repack":
                self.handleRepackSettings()

            if funcName in ["merge", "alcaHarvesting"]:
                self.handleSingleCoreOverride()

            if socket.getfqdn().endswith("cern.ch"):
                self.handleSpecialCERNMergeSettings(funcName)
        else:
            self.logger.info("DEBUG: Now in the none scenario to load PSET")
            try:
                self.loadPSet()
            except Exception as ex:
                self.logger.exception("Error loading PSet:")
                raise ex

        dummyTweak = PSetTweak()
        procScript = "edm_pset_tweak.py"
        dummypsetTweakJson = os.path.join(self.stepSpace.location, "PSetTweak_dummy.json")
        dummyTweak.persist(dummypsetTweakJson, formatting='simplejson')

        cmd = "%s --input_pkl %s --output_pkl %s --json %s" % (
            procScript,
            os.path.join(self.stepSpace.location, self.configPickle),
            os.path.join(self.stepSpace.location, "PSet_before.pkl"),
            dummypsetTweakJson)
        self.scramRun(cmd)

        # Check process.source exists
        self.logger.info("Debug: Self.process")
        self.logger.info(dir(self.process))
        if getattr(self.process, "source", None) is None and getattr(self.process, "_Process__source", None) is None:
            msg = "Error in CMSSW PSet: process is missing attribute 'source'"
            msg += " or process.source is defined with None value."
            self.logger.error(msg)
            raise RuntimeError(msg)

        self.handleCondorStatusService()
        self.fixupProcess()

        # In case of CRAB3, the number of threads in the PSet should not be overridden
        if not self.crabPSet:
            try:
                self.makeThreadsStreamsTweak()
            except AttributeError as ex:
                self.logger.error("Failed to override numberOfThreads: %s", str(ex))

        # Apply task level tweaks
        taskTweak = makeTaskTweak(self.step.data, self.tweak)
        self.applyPsetTweak(self.tweak, cleanupTweak=True)

        # Check if chained processing is enabled
        # If not - apply the per job tweaks
        # If so - create an override TFC (like done in PA) and then modify thePSet accordingly
        if hasattr(self.step.data.input, "chainedProcessing") and self.step.data.input.chainedProcessing:
            self.logger.info("Handling Chain processing tweaks")
            self.handleChainedProcessingTweak()
        else:
            self.logger.info("Creating job level tweaks")
            jobTweak = makeJobTweak(self.job, self.tweak)
        self.applyPsetTweak(self.tweak, cleanupTweak=True)

        # check for pileup settings presence, pileup support implementation
        # and if enabled, process pileup configuration / settings
        if hasattr(self.step.data, "pileup"):
            self.handlePileup()

        # Apply per output module PSet Tweaks
        self.logger.info("Output module section")
        cmsswStep = self.step.getTypeHelper()
        for om in cmsswStep.listOutputModules():
            self.logger.info("Using output module: {0}".format(om))
            mod = cmsswStep.getOutputModule(om)
            outTweak = makeOutputTweak(mod, self.job, self.tweak)
        self.applyPsetTweak(self.tweak, cleanupTweak=True)

        # revlimiter for testing
        if getattr(self.step.data.application.command, "oneEventMode", False):
            self.tweak.addParameter('process.maxEvents.input', "customTypeCms.untracked.int32(1)")

        # check for random seeds and the method of seeding which is in the job baggage
        self.handleSeeding()

        # make sure default parametersets for perf reports are installed
        self.handlePerformanceSettings()

        # fixup the dqmFileSaver
        self.handleDQMFileSaver()

        # tweak for jobs reading LHE articles from CERN
        self.handleLHEInput()

        # tweak jobs for enforceGUIDInFileName
        self.handleEnforceGUIDInFileName()

        # Check if we accept skipping bad files
        if hasattr(self.step.data.application.configuration, "skipBadFiles"):
            self.tweak.addParameter("process.source.skipBadFiles",
                "customTypeCms.untracked.bool(%s)" % self.step.data.application.configuration.skipBadFiles)

        # Apply events per lumi section if available
        if hasattr(self.step.data.application.configuration, "eventsPerLumi"):
            self.tweak.addParameter("process.source.numberEventsInLuminosityBlock",
                "customTypeCms.untracked.uint32(%s)" % self.step.data.application.configuration.eventsPerLumi)

        # limit run time if desired
        if hasattr(self.step.data.application.configuration, "maxSecondsUntilRampdown"):
            self.tweak.addParameter("process.maxSecondsUntilRampdown.input",
                "customTypeCms.untracked.PSet(input=cms.untracked.int32(%s)" % self.step.data.application.configuration.maxSecondsUntilRampdown)

        # accept an overridden TFC from the step
        if hasattr(self.step.data.application, 'overrideCatalog'):
            self.logger.info("Found a TFC override: %s", self.step.data.application.overrideCatalog)
            self.tweak.addParameter("process.source.overrideCatalog",
                "customTypeCms.untracked.string('%s')" % self.step.data.application.overrideCatalog)

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

        # check for event numbers in the producers
        self.handleProducersNumberOfEvents()

        self.logger.info("CMSSW PSet setup completed!")

        return 0
