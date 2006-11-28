#!/usr/bin/env python
"""nlog is a thin wrapper around the NetLogger package
to allow simpler usage of the NetLogger package.


We intend to implement the NetLogger methodology
as described at http://dsd.lbl.gov/NetLogger/methodology.html.


    Before running anything that uses nlog, you *MUST*
    run logd.py.  logd.py spawns the daemons for
    forwarding logs and receiving logs over the network.

    *IF LOGD.PY IS NOT RUN, YOUR LOGS WILL NOT BE RECEIVED.*

    This is the biggest change from Michelle's original
    logging method.  And of course, if you're just debugging,
    you might not want to bother forwarding to a central log-
    instead, you can always just read the local one.

    Also, there is now an extra, optional parameter to log(...),
    but that means that log remains backwards-compatible.

    However, it is strongly recommended that you read style
    suggestions below, as this will allow us to have an easier,
    more useful time visualizing logs and extracting useful info.
    

    log(message, priority=INFO, **extraData)
    ----------------------------------------
    where priority is one of the constants defined in this
    module and extraData are field-value pairs
    (i.e. a variable number of parameters of the form
    key1=value1, key2=value2,...)

    These field-value pairs, along with the message (in NL
    parlance, an 'event' - which has an implicit field-name
    'EVENT') will be used for visualization.


    STYLE SUGGESTONS
    -----------------

    Good message (event) names are ones like:

      'myModule.doBigCalculation.start'
      'myModule.doBigCalculation.end'

    for diagnostics, or:
    
      'myModule.MPI_Comm_World.err'

    for an error.
    

    Good field-value pairs might be:
      MPI_ERROR_CODE=32,
      ERR_MESSAGE='Looks like our MPI Universe is too small for spawning!' }

    for an error log

    or 

      LOOP_ITERATOR_VALUE=i, MY_MPI_RANK=mpiRank

    in an very computationally-intensive loop on variable i
    inside an MPI program.


    Note that all logs will automatically include the calling node's hostname,
    the current system time.  And of course, bear in mind when writing
    an error message, we can filter out any information we want,
    but obviously cannot extract information that is not given!


    code examples:

    import nlog
    ....

    def main(argv):

        nlog.log('myModule.main.start', nlog.INFO)
        ....

        try: 
            for i in range(10):
                nlog.log('myModule.moveHugeDataSet.start',
                            nlog.INFO,
                            HUGE_DATA_SET_INDEX=i)
                moveHugeDataSet(i)
                nlog.log('myModule.moveHugeDataSet.end',
                            nlog.INFO,
                            HUGE_DATA_SET_INDEX=i)
        except e:
            nlog.log('myModule.main.err', nlog.ERR,
                        ERR_MESSAGE=\"\"\"Got an exception
                        while moving huge data sets!\"\"\")
        ...

        ...

        nlog.log('myModule.main.end', nlog.INFO)
        return 0



    also note that log(...) can take a parameter,
    fieldDict, which should be a dictionary.  This is useful
    if you might have a set of field-value pairs which are
    frequently reused, e.g. instead of

    nlog.log(\'main.start\', PROGNAME=myCode, SOURCEFILE=myCode.py)
      ...
    nlog.log(\'main.middle.start\', PROGNAME=myCode, SOURCEFILE=myCode.py)
      ...
    nlog.log(\'main.middle.end\', PROGNAME=myCode, SOURCEFILE=myCode.py)
                         

    you might use

    loggingDict = { \'PROGNAME\':\'myCode\', SOURCEFILE=\'myCode.py\'}

    nlog.log(\'main.start\', fieldDict=loggingDict)
     ....
    nlog.log(\'main.middle.start\', fieldDict=loggingDict)
     ....
    nlog.log(\'Rather terrible error!\', priority=nlog.CRITICAL,
             fieldDict=loggingDict, ERRCODE=32)




ISSUES:
    Right now we are using very simple policy files.  They specificy
    a single log file name for local logs - this is somewhat dangerous,
    as log files will clobber each other.

    Thus, as it stands, the actual log file written is not the one
    specified in the policy file, but actually that filename.<PID>.<host IP>

    Thus, in our PBS script, we also have to forward:
        filename.*
    to our central logger.

    This doesn't really hurt anything, but it's a bit of a hack and
    will hopefully change in the future.
        
Added by jmyers.
"""




from gov.lbl.dsd.netlogger import nllite
import os
import sys
import lsst.apps.fw.Policy.Policy as Policy

__author__="jmyers"



#For API compatibility with Michelle's version

CRIT = nllite.Level.FATAL
CRITICAL = nllite.Level.FATAL
ERR = nllite.Level.ERROR
ERROR = nllite.Level.ERROR
WARNING = nllite.Level.WARN
INFO = nllite.Level.INFO
DEBUG = nllite.Level.DEBUG

DefaultLogLevel = DEBUG

globalLogger = None

forcedPid = None






def setForcedPid(newForcedPid):
    """setForcedPid:
    input:
    newForcedPid (number):  pid which nlog will use
    for generation of logfile name.
    
    Calling this function will cause nlog.log to write to a logfile
    using newForcedPid in the filename.  This is useful for
    limiting the number of logfiles created, but should only
    be used if you are sure that the *actual* owner of the
    forced PID will not be writing simultaneously, as this
    can cause clobbering.
    
    The forced pid behavior can be undone with unsetForcedPid.
    """
    global forcedPid
    try:
        forcedPid = int(newForcedPid)
    except:
        sys.stderr.write("nlog.setForcedPid: Unexpected error while running with paramater "
                         + newForcedPid+"\n")






def unsetForcedPid():
    """ See setForcedPid for more information.
    
    This function causes the forced-PID functionality to be
    turned off."""
    global forcedPid
    forcedPid = None






def getPolicy():
    """ return a Policy class built from the logging.policy file,
    and if this fails, return None."""
    logPolicy = None
 
    # first look for policy in directory pointed to by env:$LSST_POLICY_DIR
    policyDir = os.getenv("LSST_POLICY_DIR")
    if policyDir != None and os.path.isfile(os.path.join(policyDir, "logging.policy")):
        logPolicy = Policy("logging.policy")
    else:
        #check nlog.pyc's exe dir:<sndbox>/lib/python/lsst/mw/dps/pipeexec/nlog/
        modPath = os.path.split(__file__)[0]
        policyFile = os.path.join(modPath, "logging.policy")
        if os.path.isfile(policyFile):
            logPolicy = Policy(policyFile)
    return logPolicy







def getLogFileName(policyObj, forcePid=None):
    """getLogFileName:
       takes:
       -a policy object opened on the logging policy file.
       -forcePid: Optional. If provided, the log file will use this PID for
        the generation of a unique log file name instead of the current PID.
        Use at your own discretion; this can possibly cause the clobbering of
        your log files.

       returns: a unique (across the cluster) logging filename based
        on the logFile entry in the policy file,
        i.e. <logFile>.<username>.<PID>.<host IP>

        if there is currently a forcedPid in effect (other than the one
        specified by forcePid, the optional parameter), then that
        will be used for the PID field.  See setForcedPid and
        unsetForcedPid.


        """
    global forcedPid
    if policyObj == None:
       return '&' #NetLogger-ese for "stderr"
    baseName = policyObj.get("logFile")
    if os.getenv("USER") == None:
        sys.stderr.write("WARNING: nlog.py: got $USER == None!\n")
    if forcePid == None:
        if forcedPid == None:
            logPid = os.getpid()
        else:
            logPid = forcedPid
    else:
        logPid = forcePid
    return baseName + "." + str(os.getenv("USER")) + "." + str(logPid) + "." + str(nllite.get_host())


def getLogFileGlob(policyObj):
    """getLogFileGlob:
       takes: a policy object instantiated on the logging policy file.

       Returns: A UNIX glob which will match all file names returned by
       getLogFileName, or, if policyObj is None (there is no policy in use)
       return an empty string."""
    if policyObj == None:
       return "" #We're using stderr, so don't forward anything anyway
    baseName = policyObj.get("logFile")
    if os.getenv("USER") == None:
        sys.stderr.write("WARNING: nlog.py: got $USER == None!\n")
    return baseName  + "." + str(os.getenv("USER")) + ".*.*"
    

def log(message, priority=DefaultLogLevel, source="", forcePid=None, fieldDict=None, **extraData):
    """log:  Log a message. (with NetLogger!)


    Please read nlog.py's docstring for more info
    on using this utility!


    Inputs:
      -message: an event which has occurred or error message.
      -priority:  message priority, one of:
        - CRIT (or CRITICAL)
        - ERR (or ERROR)
        - WARNING
        - INFO
        - DEBUG
        The meanings are the same as for the syslog unix utility
      -source: Optional, the name of your program.
      -forcePid: Optional. If provided, the log file will use this PID for
        the generation of a unique log file name instead of the current PID.
        Use at your own discretion; this can possibly cause the clobbering of
        your log files.
      -fieldDict:  A Python Dictionary of field-value pairs
        which should be used for specific data you would like to record.
      -extraData: Any number of Name=value parameters which will be
        logged.
        
        Please see the nlog module's docstring for much better
        style and usage information on this one, it's very important!
      

    -jmyers
    """
    global globalLogger
    ed = extraData
    if fieldDict != None:
        ed.update(fieldDict)
    if source != "":
        ed['SOURCE'] = source
    ed['MYPID'] = os.getpid()

    logPolicy = getPolicy();
    localLog = getLogFileName(logPolicy, forcePid=forcePid)
    if globalLogger == None:
        globalLogger = nlogClass(localLog)
    elif globalLogger.getLogFile().name != localLog:
        #check that we are logging with the correct logFile, otherwise update
        globalLogger.close()
        globalLogger = nlogClass(localLog)

    globalLogger.write(message, extraData=ed, msgLevel=priority)



def withoutWhiteSpace(input):
    returnVal = input
    if isinstance(input, str):
        returnVal = input.replace(' ', '_')
    return returnVal




#### actual nlogClass Class

class nlogClass:
    """nlogClass is a thin wrapper around the NetLogger package
    to allow (slightly) simpler, non-buffered logging for Python.

    This class is used by nlog.py's log function.

    Unless you're doing something particularly special,
    nlog.py's log function is probably what you really want.

    The nlogClass class basically does the following for you:
    -when instantiated:
      -create an nllite (NetLogger) LogOutputStream
      -sets the local logfile for the LogOutputStream
      -sets the LogOutputStream loglevel to Debug3 (highest)
      -if second parametre, centralLogNodeAddress is given to
         __init__:
         starts a nlforward daemon for the local logfile,
         which watches the local log for activity and periodically
         forwards data to the central log node.  It is
         suggested, however, that a single nlforward daemon is
         and a single log file are used for the entire application.
         
    -when writing, it also:
       -always adds, if not present, a HOST='hostname' field/value
       -flushes the buffer every time a message is written

    TODO:
    KNOWN BUGS:
    if you try to send two log messages to the same nlogClass and they
    have the same message string but an extraData dict with more items,
    you inexplicably get a seg fault.  This happens after the call to
    NetLogger's write, so it can't be my fault.  Nonetheless, it's
    hideous and we'll just use a new nlogClass every time we need to write.

    Added by jmyers.
    """



    
    def __init__(self, localLogfile):
        """Params:
           localLogFile: temporary log file on local machine."""
        global DefaultLogLevel
        self.nlLOS = nllite.LogOutputStream(localLogfile, c_fmt=False)
        self.nlLOS.setLevel(DefaultLogLevel) #report all logs

    def close(self):
        self.nlLOS.close()
        self.nlLOS._logfile.close()

    def getLogFile(self):
        return self.nlLOS._logfile



    def write(self,eventString,extraData={},msgLevel=INFO):
        """Writes the eventString to the log associated with this 
        object. Automatically includes the timestamp and the name
        of the host in the log.

        Optionally adds other FIELD=DATA pairs from the
        extraData parameter.


        Params:
            eventString:
              name of event, e.g. function foo should have
              foo.start and foo.end as events written
              at the start and end of the function,
              and an error might have BAD_FILE_DESCRIPTOR
              as an eventString.
            msgLevel:  
              INFO by default, take one from the Level class
              in the LSSTData module (FATAL, ERROR, WARN,
              INFO, DEBUG, DEBUG1, DEBUG2, DEBUG3)
            extraData:
              A Python dictionary  of field/value pairs
              to be included in the log for analysis
              purposes.  E.G.:
                {'ITERATION':'304', 'CURRENTFILE':'/tmp/3016'}
                or
                {'ERRNUM':'501', 'COREDUMPLOCATION':'~/doom'}"""
        if 'HOST' not in extraData.keys():
            extraData['HOST'] = nllite.get_host()
        #remove spaces, NLV doesn't like them!
        noSpaceEventString = withoutWhiteSpace(eventString)
            
        noSpaceDict = {}
        for i in extraData:
            noSpaceDict[withoutWhiteSpace(i)] = withoutWhiteSpace(
                extraData[i])
        
        #print "nlog.py: netlogger.write(", noSpaceEventString, ", ",\
        #      noSpaceDict, ", level=",msgLevel, ")"
        self.nlLOS.write(noSpaceEventString, noSpaceDict, level=msgLevel)
        self.nlLOS.flush()


if __name__ == "__main__":
    #unit tests
    log("this is a test message: hello, world", priority=DEBUG, SKY="blue", EARTH="green")
    #testing for same-message, different-structure error
    log("this is a test message: hello, world", priority=DEBUG, SKY="blue", EARTH="green", CLOUDS="white")
    log("Fake error", priority=ERROR, SKY="green")
    log("Fake error", forcePid=1, priority=ERROR, SKY="green")
    setForcedPid(1337)
    log("Message from mr. 1337", MYNAME="sweet")
    unsetForcedPid()
    log("Message from the real PID", MYNAME="so-so")
#    for i in range(1000000):
#        log("lots o logs", i=i)

