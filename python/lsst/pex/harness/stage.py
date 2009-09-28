import os, sys, re
from Queue import Queue

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

    def getRun(self):
        """
        get the hostname where the event broker currently in use is located
        """
        return self.runId

    def getRank(self):
        """
        get the hostname where the event broker currently in use is located
        """
        return self.rank

    def getUniverseSize(self):
        """
        get the hostname where the event broker currently in use is located
        """
        return self.universeSize

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
        clipboard = self.inputQueue.getNextDataset()
        self.preprocess(clipboard)
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
        This impementation loops over the clipboards on the given InputQueue, 
        calls postprocess() on each one, and posts it to the stage outputQueue.

        @param queue  the InputQueue instance returned by applyPreprocess()
        """
        while queue.size() > 0:
            clipboard = queue.getNextDataset()
            self.postprocess(clipboard)
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
        clipboard = self.inputQueue.getNextDataset()
        self.process(clipboard)
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
    constitute a stage according to policy configuration.

    The policy this class expects is the part of a typical pipeline policy
    that describes one stage of a pipeline with an "appStage" parameter. The
    expected named contents of this policy are:
      name           a short name for the stage meant for display purposes
      serialClass    the fully qualified name for the SerialProcessing
                         component
      parallelClass  the fully qualified name for the SerialProcessing 
                         component
      stagePolicy    the stage-specific policy data that configures the
                         stage
      eventTopic     the topic name of an event that must be received and
                         its contents placed on a clipboard in order for
                         the stage to process the clipboard's contents
    In general, all of these are optional; however, specific stages will
    require some or all of these to be set in order to run as intended.

    This class has two typical uses.  The first is simply as a factory class
    that generates new instances of the SerialProcessing and
    ParallelProcessing classes; the create*() functions support this.
    These are intended for use by the Pipeline and Slice classes.  The second
    use is to actually process data through the stage in a non-parallel,
    scripting or interactive context; the ...
    """

    def __init__(self, policy, log=None, eventBroker=None, sysdata=None):
        """
        initialize this stage with the policy that defines the stage and
        some contextual system data.
        @param policy       the "appStage" policy data that defines the stage.
                                (see class documentation above for details).
        @param stageId      the integer identifier for this stage within the 
                               sequence of stages that makes up a pipeline.
                               Default=-1
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
        self.sysdata = sysdata.copy()
        
        self.appStagePolicy = policy
        if self.appStagePolicy.exists("name"):
            self.sysdata["name"] = self.appStagePolicy.get("name")
        if not sysdata.has_key("name"):
            self.sysdata["name"] = "unknown stage"
        self._safename = re.sub(r'\W+', '', self.sysdata)

        self.eventBroker = eventBroker
        if log is None:
            log = Log(Log.getDefaultLog(), "stage")
        self.log = log

        self._serialPart = None
        self._parallPart = None
        
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
        stageClass = None
        if self.appStagePolicy.exists("serialClass"):
            stageClass = self.appStagePolicy.get("serialClass")
        return self._create(stageClass, -1, log, extraSysdata, True)

    def createParallelProcessing(self, rank=1, log=None, extraSysdata=None):
        """
        create a new ParallelProcessing instance.

        All of the parameters are optional.  If not provided, they will be
        set to the values provided by the constructor (except for rank).
        @param rank     the integer identifier for the parallel thread 
                           that hosts this instance.  Default=-1.
                           indicating the master thread for the pipeline.
        @param stageId  the integer identifier for this stage within the 
                           sequence of stages that makes up a pipeline.
                           Default=-1
        @param log      the log object the stage instance should stage
                          should use.  If not provided, a default will be
                          used.
        @param sysdata  a dictionary of arbitrary contextual system that
                          the stage might need.  
        @param runId    the string identifier for the production run that
                          this stage is a part of.  If None, a default will
                          be set.
        """
        stageClass = None
        if self.appStagePolicy.exists("parallelClass"):
            stageClass = self.appStagePolicy.get("parallelClass")
        return self._create(stageClass, rank, log, extraSysdata, False)

    def _create(self, className, rank, log, extraSysdata, isSerial):
        sysdata = self.sysdata.copy()
        for k in extraSysdata.keys():
            sysdata[k] = extraSysdata[k]
        sysdata["rank"] = rank

        policy = None
        if self.appStagePolicy.exists("stagePolicy"):
            policy = self.appStagePolicy.getPolicy("stagePolicy")

        if log is None:
            log = self.log

        if className:
            (modn, cln) = self.serialClass.rsplit('.', 1)
            mod = __import__(modn, globals(), locals(), [cln], -1)
            stageClass = getattr(mod, cln)

            out = stageClass(policy, log, self.eventBroker, sysdata)
        elif isSerial:
            out = NoOpSerialProcessing(policy, log, self.eventBroker, sysdata)
        else:
            out = NoOpParallelProcessing(policy,log,self.eventBroker, sysdata)

        return out

    
