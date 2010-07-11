# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import os, sys, re
from Queue import Queue
from lsst.pex.logging import Log

class StageProcessing(object):
    """
    A container for processing that handles one step in a pipeline.  This
    container will be realized either via the subclass, SerialProcessing
    (when held by a Pipeline) or via the subclass, ParallelProcessing (when
    held by a Slice)
    
    This abstract class is implemented (via one of the subclasses,
    SerialProcessing or ParallelProcessing) to apply a specific
    science-oriented algorithm to data drawn from the queue.
    """

    def __init__(self, policy=None, log=None, eventBroker=None, sysdata=None,
                 callSetup=True):
        """
        initialize this stage.  This will call setup() after all internal
        attributes are initialized unless callSetup is False.

        The sysdata is a dictionary intended to include data describing 
        the execution context to assume for this instance.  This constructor 
        will recognize the following named items:
           name      a string name for display purposes that identifies 
                        this stage does for the pipeline.  If not provided, 
                        a default name will be set.
           stageId   the integer identifier for this stage within the 
                        sequence of stages that makes up a pipeline.
                        A value of -1 means that the stage is not part of
                        a pipeline sequence
           rank      the integer identifier for the parallel thread that
                        hosts this instance.  A value of -1 indicates the 
                        master thread for the pipeline.
           runId     the string identifier for the production run that
                        this stage is a part of.  If None, a default will
                        be set.
           universeSize   the total number of parallel threads

        The dictionary may contain other arbitrary data.  In general,
        constructors for specific stage subclasses that provide application
        algorithms will not be able to make use of this information without
        special knowledge of the execution context.  
        
        @param policy       the policy for configuring this stage.  This may 
                              be None if this stage does not require a policy
                              for configuration.
        @param log          the log object the stage instance should stage
                              should use.  If not provided, a default will be
                              used.
        @param eventBroker  the name of the host where the event broker is
                              running.  If not provided, an eventBroker will
                              not be available to the stage.
        @param sysdata      a dictionary of data describing the execution
                              context.  The stage uses this information to
                              set some of its internal data.  
        @param callSetup    if true, the setup() function will be called as the
                              the last step in this constructor, after all data
                              is initialized.  Default: True.
        """

        # the policy that configures this stage
        self.policy = policy

        # arbitrary data describing the execution context
        if sysdata is None:
            sysdata = {}
        self.sysdata = sysdata.copy()

        # the integer identifier for this stage within the sequence of stages
        # that makes up a pipeline.
        self.stageId = -1
        if sysdata.has_key("stageId"):
            self.stageId = sysdata["stageId"]

        # the integer identifier for the parallel thread that hosts this 
        # instance 
        self.rank = -1
        if sysdata.has_key("rank"):
            self.rank = sysdata["rank"]

        # the string identifier for the production run that this stage is a
        # part of
        self.runId = "user-driven"
        if sysdata.has_key("runId"):
            self.runId = sysdata["runId"]

        # a string name for display purposes that identifies this stage does
        # for the pipeline.
        self.name = "processing stage"
        if sysdata.has_key("name"):
            self.name = sysdata["name"]

        # the total number of parallel threads within the pipeline hosting
        # this stage
        self.universeSize = 1
        if sysdata.has_key("universeSize"):
            self.universeSize = sysdata["universeSize"]

        # the host where the event broker is running.  If None, then an
        # event broker is not available.
        self.eventBroker = eventBroker

        # the root log stages should derive their log instances from
        self.log = log

        # the input queue (used in scripting mode)
        self.inputQueue = None
        
        # the output queue (used in scripting mode)
        self.outputQueue = None
        
        if callSetup:
            self.setup()

    def setup(self):
        """
        setup the initial, internal state for this stage.  This default
        implementation does nothing; however, subclasses may override it.
        It will be called during construction after this parent class's
        attributes are initialized (unless the constructor is called with
        callSetup=False).
        """
        pass

    def initialize(self, outQueue, inQueue):
        """
        called once when the stage is plugged into a pipeline by the manager
        of the pipeine (either a Pipeline or Slice instance), this method
        initializes the stage so that it is ready to process datasets.  It
        passes in the data queues it will use.  Subclasses may override this
        but should call the parent's implementation.
        """
        self.outputQueue = outQueue
        self.inputQueue  = inQueue

    def getEventBrokerHost(self):
        """
        get the hostname where the event broker currently in use is located
        """
        return self.eventBroker

    def setRun(self, runId):
        """
        set the runid 
        """
        self.runId = runId

    def getRun(self):
        """
        get the runid
        """
        return self.runId

    def setRank(self, rank):
        """
        set the MPI rank of the process running this stage
        """
        self.rank = rank

    def getRank(self):
        """
        get the MPI rank of the process running this stage
        """
        return self.rank

    def setUniverseSize(self, universeSize):
        """
        set the MPI universe size
        """
        self.universeSize = universeSize

    def getUniverseSize(self):
        """
        get the MPI universe size, an integer
        """
        return self.universeSize

    def getName(self):
        """
        return the name assigned to this stage.
        """
        return self.name

    def shutdown():
        """
        discontinue processing and clean up and release resources.
        """
        pass


class SerialProcessing(StageProcessing):
    """
    The container for the serial part of the processing that happens before 
    and after the parallel part.  This processing will happen in the context 
    of a Pipeline.
    """
    
    def __init__(self, policy=None, log=None, eventBroker=None,
                 sysdata=None, callSetup=True):
        """
        initialize this stage.  This will call setup() after all internal
        attributes are initialized unless callSetup is False.

        @param policy       the policy for configuring this stage.  This may 
                              be None if this stage does not require a policy
                              for configuration.
        @param log          the log object the stage instance should 
                              should use.  If not provided, a default will be
                              used.
        @param eventBroker  the name of the host where the event broker is
                              running.  If not provided, an eventBroker will
                              not be available to the stage.
        @param sysdata      a dictionary of data describing the execution
                              context.  The stage uses this information to
                              set some of its internal data.  See
                              StageProcessing documentation for datails.  
        @param callSetup    if true, the setup() function will be called as the
                              the last step in this constructor, after all data
                              is initialized.  Default: True.
        """
        StageProcessing.__init__(self, policy, log,eventBroker, sysdata, False)
        if callSetup:
            self.setup()


    def applyPreprocess(self):
        """
        Apply the preprocess() function to data on the input queue.
        Returned is an InputQueue that contains the clipboards processed
        by this function and which should be passed onto applyPostprocess().
    
        This implementation retrieves a single clipboard from the input
        queue and preprocess it.  While most subclasses will inherit this
        implementation, some may override it to take more control over the
        processing data from the input queue.
        """
        # Don't pop it off because failureStage will then not be able to access it
        # element() gives a reference
        clipboard = self.inputQueue.element()
        self.preprocess(clipboard)
        # Pop it off at this point; a new reference is not needed, so it is a dummy
        dummyClipboard = self.inputQueue.getNextDataset()
        out = Queue()
        out.addDataset(clipboard)
        return out

    def preprocess(self, clipboard):
        """
        execute the serial processing that should occur before the parallel
        processing part of the stage on the data on the given clipboard.

        @param clipboard   the data to process, packaged as a Clipboard
        """
        raise RuntimeError("Not Implemented: preprocess()")

    def applyPostprocess(self, queue):
        """
        apply the postprocess() function to an ordered set of clipboards.  
        This implementation loops over the clipboards on the given InputQueue, 
        calls postprocess() on each one, and posts it to the stage outputQueue.

        @param queue  the InputQueue instance returned by applyPreprocess()
        """
        while queue.size() > 0:
            # Don't pop it off because failureStage will then not be able to access it 
            # clipboard = queue.getNextDataset()
            clipboard = queue.element()
            self.postprocess(clipboard)
            # Pop it off at this point; a new reference is not needed, so it is a dummy
            dummyClipboard = queue.getNextDataset()
            self.outputQueue.addDataset(clipboard)
        

    def postprocess(self, clipboard):
        """
        execute the serial processing that should happen after the parallel
        processing part of the stage.

        @param clipboard   the data to process, packaged as a Clipboard
        """
        raise RuntimeError("Not Implemented: postprocess()")


class ParallelProcessing(StageProcessing):
    """
    a container class for the parallel processing part of a pipeline stage.  
    This processing will happen in the context of a Slice.
    """
    
    def __init__(self, policy=None, log=None, eventBroker=None,
                 sysdata=None, callSetup=True):
        """
        initialize this stage.  This will call setup() after all internal
        attributes are initialized unless callSetup is False.

        @param policy       the policy for configuring this stage.  This may 
                              be None if this stage does not require a policy
                              for configuration.
        @param log          the log object the stage instance should stage
                              should use.  If not provided, a default will be
                              used.
        @param eventBroker  the name of the host where the event broker is
                              running.  If not provided, an eventBroker will
                              not be available to the stage.
        @param sysdata      a dictionary of data describing the execution
                              context.  The stage uses this information to
                              set some of its internal data.  See
                              StageProcessing documentation for datails.  
        @param callSetup    if true, the setup() function will be called as the
                              the last step in this constructor, after all data
                              is initialized.  Default: True.
        """
        StageProcessing.__init__(self, policy, log, eventBroker, sysdata, False)
        if callSetup:
            self.setup()


    def applyProcess(self):
        """
        apply the process() function to data from the input queue.  This
        implementation will pull one clipboard from the input queue, call 
        process() on it, and post it to the output queue.  While most 
        subclasses will inherit this default implementation, some may 
        override it to take more control over how much data to process.
        """
        # Don't pop it off because failureStage will then not be able to access it 
        # clipboard = self.inputQueue.getNextDataset()
        clipboard = self.inputQueue.element()
        self.process(clipboard)
        # Pop it off at this point; a new reference is not needed, so it is a dummy
        dummyClipboard = self.inputQueue.getNextDataset()
        self.outputQueue.addDataset(clipboard)

    def process(self, clipboard):
        """
        execute the parallel processing part of the stage within one thread 
        (Slice) of the pipeline.
        """
        raise RuntimeError("Not Implemented: process()")


class NoOpSerialProcessing(SerialProcessing):
    """
    A SerialProcessing subclass that provides no-op implementations
    of preprocess() and postprocess().

    The default SerialProcessing implementations normally through Runtime
    exceptions.
    """

    def preprocess(self, clipboard):
        """
        execute the serial processing that should occur before the parallel
        processing part of the stage on the data on the given clipboard.

        @param clipboard   the data to process, packaged as a Clipboard
        """
        pass

    def postprocess(self, clipboard):
        """
        execute the serial processing that should happen after the parallel
        processing part of the stage.

        @param clipboard   the data to process, packaged as a Clipboard
        """
        pass




class NoOpParallelProcessing(ParallelProcessing):
    """
    A ParallelProcessing subclass that provides a no-op implementation
    of process().

    The default ParallelProcessing implementation normally throws a Runtime
    exception.
    """

    def process(self, clipboard):
        """
        execute the parallel processing part of the stage within one thread 
        (Slice) of the pipeline.

        @param clipboard   the data to process, packaged as a Clipboard
        """
        pass

class Stage(object):
    """
    a class that will create and initialize the StageProcessing classes that
    constitute a stage.

    There three intended ways to create a Stage instance.  First is by
    the makeStageFromPolicy() module function.  Intended for use inside the
    Pipeline-constructing classes (like Pipeline and Slice), this method
    takes as input a policy that provides the names of the SerialProcessing
    and ParallelProcessing classes that define the stage.  Alternatively, 
    one can pass these names explicitly via the makeStage() module function.
    The third way is by directly constructing a subclass of Stage.  The
    simplest way to create this subclass is as follows:

        class MyStage(Stage):
            serialClass   = MySerialProcessing
            parallelClass = MyParallelProcessing

    If the stage does not have a parallel component, then the 'parallelClass'
    variable does not need to be updated; likewise, for serial component.
    """

    serialClass   = NoOpSerialProcessing
    parallelClass = NoOpParallelProcessing

    def __init__(self, policy, log=None, stageId=-1, eventBroker=None,
                 sysdata=None):
        """
        initialize this stage with the policy that defines the stage and
        some contextual system data.  Applications normally do not directly
        call this constructor.  Instead they either construct a Stage subclass
        or create a Stage instance using makeStage() or makeStageFromPolicy().
        
        @param policy       the policy that will configure the SerialProcessing
                               and ParallelProcessing
        @param log          the log object the stage instance should 
                              should use.  If not provided, a default will be
                              used.
        @param eventBroker  the name of the host where the event broker is
                              running.  If not provided, an eventBroker will
                              not be available to the stage.
        @param sysdata      a dictionary of data describing the execution
                              context.  The stage uses this information to
                              set some of its internal data.  See
                              StageProcessing documentation for datails.
                              The name provided in the policy will override
                              the name in this dictionary
        """
        if sysdata is None:
            sysdata = {}
        self.sysdata = sysdata
        self.stagePolicy = policy
        self.eventBroker = eventBroker
        if log is None:
            log = Log(Log.getDefaultLog(), "stage")
        self.log = log

    def setLog(self, log):
        """
        set the logger that should be assigned to the stage components when
        they are created.
        """
        self.log = log

    def setEventBroker(self, broker):
        """
        set the instance of the event broker that the stage components should
        use.
        """
        self.eventBroker = broker

    def updateSysProperty(self, sysdata):
        """
        override the current execution context data that will be passed to
        the stage components when they are created with the given data.
        Currently set data with names that are not included in the given
        dictionary will not be affected.  
        """
        if not sysdata:
            return
        for name in sysdata.keys():
            self.sysdata[name] = sysdata[name]

    def getSysProperty(self, name):
        """
        return the currently set value for the execution context property or
        None if it is not set.
        @param name   the name of the property
        """
        return self.sysdata.get(name, None)

    def createSerialProcessing(self, log=None, extraSysdata=None):
        """
        create a new SerialProcessing instance.

        All of the parameters are optional.  If not provided, they will be
        set to the values provided by the constructor.
        @param stageId      the integer identifier for this stage within the 
                               sequence of stages that makes up a pipeline.
                               Default=-1
        @param log          the log object the stage instance should stage
                              should use.  If not provided, a default will be
                              used.
        @param extraSysdata a dictionary of execution context data whose items
                              will override those set via the Stage constructor
        """
        if log is None:
            log = self.log
        return self._create(self.serialClass, -1, log, extraSysdata)

    def createParallelProcessing(self, rank=1, log=None, extraSysdata=None):
        """
        create a new ParallelProcessing instance.

        All of the parameters are optional.  If not provided, they will be
        set to the values provided by the constructor (except for rank).
        @param rank     the integer identifier for the parallel thread 
                           that hosts this instance.  Default=-1.
                           indicating the master thread for the pipeline.
        @param log      the log object the stage instance should stage
                          should use.  If not provided, a default will be
                          used.
        @param sysdata  a dictionary of arbitrary contextual system that
                          the stage might need.  
        @param runId    the string identifier for the production run that
                          this stage is a part of.  If None, a default will
                          be set.
        """
        if not log:
            log = self.log
        return self._create(self.parallelClass, rank, log, extraSysdata)

    def _create(self, cls, rank, log, extraSysdata):
        # set the rank into the system data
        sysdata = self.sysdata.copy()
        for k in extraSysdata.keys():
            sysdata[k] = extraSysdata[k]
        sysdata["rank"] = rank

        if log is None:
            log = self.log

        return cls(self.stagePolicy, log, self.eventBroker, sysdata)

def _createClass(name):
    (modn, cln) = name.rsplit('.', 1)
    mod = __import__(modn, globals(), locals(), [cln], -1)
    stageClass = getattr(mod, cln)
    return stageClass
    
def makeStageFromPolicy(stageDefPolicy, log=None, eventBroker=None,
                        sysdata=None):
    """
    create a Stage instance from a "stage definition policy".  This policy
    corresponds to the "appStage" parameter in a pipeline policy file that
    defines a pipeline (described below).  The resulting instance is used
    as the factory for creating instances of SerialProcessing and
    ParallelProcessing.

    The stage definition policy (the "appStage" parameter) has the following
    contents:
    
      name             a short name identifying the name (e.g. with logs)
      serialClass      a string containing the fully qualified
                         SerialProcessing class name.  If the stage does not
                         have a serial component, this parameter is not
                         provided.
      parallelClass    a string containing the fully qualified
                         ParallelProcessing class name.  If the stage does not
                         have a parallel component, this parameter is not
                         provided.
      eventTopic       the name of an event topic that the stage expects to
                         be received and placed on the clipboard.  If no
                         event is expected, this parameter is not provided.
      stagePolicy      the policy that configures the stage and thus should
                         be passed to the SerialProcessing and
                         ParallelProcessing constructors.

    @param stageDefPolicy  the policy for defining this stage (see above).  
    @param log          the log object the stage instance should stage
                          should use.  If not provided, a default will be
                          used unless one is provided via setLog() on the
                          returned Stage instance.
    @param eventBroker  the name of the host where the event broker is
                          running.  If not provided, an eventBroker will
                          not be available to the stage unless setEventBroker()
                          is called on the returned Stage instance.
    @param sysdata      a dictionary of data describing the execution
                          context.  The stage uses this information to
                          set some of its internal data.  
    """
    if sysdata is None:
        sysdata = {}
    mysysdata = sysdata
    
    if not mysysdata.has_key("name"):
        mysysdata = sysdata.copy()
        if stageDefPolicy.exists("name"):
            mysysdata["name"] = stageDefPolicy.get("name")
        else:
            mysysdata["name"] = "unknown stage"

    stageConfigPolicy = stageDefPolicy.get("stagePolicy")

    serialClass = None
    parallelClass = None
    if stageDefPolicy.exists("serialClass"):
        serialClass = stageDefPolicy.get("serialClass")
    if stageDefPolicy.exists("parallelClass"):
        parallelClass = stageDefPolicy.get("parallelClass")

    return makeStage(stageConfigPolicy, serialClass, parallelClass,
                     log, eventBroker, mysysdata)


def makeStage(policy=None, serClsName=None, paraClsName=None, log=None,
              eventBroker=None, sysdata=None):
    """
    create a Stage instance that is defined by a SerialProcessing class
    and/or a ParallelProcessing class.
    @param policy       the stage configuration policy.  This policy will be
                          passed to the SerialProcessing and ParallelProcessing
                          class constructors.  
    @param log          the log object the stage instance should stage
                          should use.  If not provided, a default will be
                          used unless one is provided via setLog() on the
                          returned Stage instance.
    @param eventBroker  the name of the host where the event broker is
                          running.  If not provided, an eventBroker will
                          not be available to the stage unless setEventBroker()
                          is called on the returned Stage instance.
    @param sysdata      a dictionary of data describing the execution
                          context.  The stage uses this information to
                          set some of its internal data.  
    """
    out = Stage(policy, log, eventBroker, sysdata)

    if serClsName:
        out.serialClass = _createClass(serClsName)
        if not issubclass(out.serialClass, SerialProcessing):
            raise ValueError("Not a SerialProcessing subclass: " + serClsName)
    if paraClsName:
        out.parallelClass = _createClass(paraClsName)
        if not issubclass(out.parallelClass, ParallelProcessing):
            raise ValueError("Not a ParallelProcessing subclass: "+paraClsName)

    return out
