#!/usr/projects/LSST/DC1/python-2.4.2/python-2.4.2/bin/python
"""nlogtopd.py:

This program is intended to be run in the background.
It will manage most of the NetLogger-ly behavior, such as
reading the policy file, setting the local logfile accordingly,
setting up a forwarding daemon for the local file, and setting up
our logging utility to write to that file every time we call
log(message).  Thus, it needs to be running on every machine which
does logging.

However, if it is not run, logs will all be written to stderr, and
a warning to this effect will be written to stdout when a log
is first written.

In addition, if nlogtopd is run, it will write handy top-style
system information to the log so we can monitor processes on
the local machine.

TODO:  Fix the $TERM situation - top wants a $TERM variable set
even for batch mode.  It looks like this is a problem with top,
however, not with us.  Maybe we could use a smarter approach?

Added by jmyers."""

import os
import time #for sleeping!
import sys
import commands
import lsst.dps.pipeexec.nlog as nlog
import lsst.fw.Policy.Policy as Policy



DEBUG=False
#parameters for handy future usage
TopFrequency=1

__author__="jmyers"





def getCpuUsage(vmstat="/usr/bin/vmstat"):
    """Requires vmstat.  Pretty much stolen from
    nlcpu.py in the NetLogger 3 package.

    Notably, vmstat on NCSA seems to constantly report
    the same value.  That's pretty odd.

    """
    f = os.popen(vmstat)
    f.readline()
    f.readline()
    line = f.readline()
    vals = map(int,line.split()[-4:-1])
    return vals[0] + vals[1] #this is OK for TeraGrid (at least at NCSA)



def getSpecificCpuUsage(vmstat="/usr/bin/vmstat"):
    """ Requires vmstat.  Returns a dictionary of
    useful CPU usage information, e.g.
    { %CPU_USR:30
      %CPU_SYS:30
      %CPU_IOWAIT:30 }

      ISSUES:  vmstat really doesn't return anything useful
      unless it iterates several times (which doesn't happen here).

      Thus this function is virtually useless until updated!

    On error (e.g. vmstat looks funny), returns None"""
    returnDict = None
    try:
        returnDict = {}
        f = os.popen(vmstat)
        f.readline()
        middleline = f.readline()
        if middleline.split()[-4:] != ["us", "sy", "id", "wa"]:
            nlog.log("nltopd.py: vmstat output doesn't look like expected - stopping", 
                     priority=nlog.ERROR, MIDDLELINE=middleline)
        line = f.readline()
        vals = map(int, line.split()[-4:])
        returnDict['CPU_USR'] = vals[0]
        returnDict['CPU_SYS'] = vals[1]
        returnDict['CPU_IOWAIT'] = vals[3]
    except IOError:
        nlog.log("nltopd.py: Unexpected error (check that vmstat on this machine is GNU procps 3.1.11-like)", priority=nlog.ERROR)

    return returnDict


def topProcessesDict(top="/usr/bin/top"):
    """topProcessesDict:

    calls the program top, then parses the heck out of its 
    output to return a dictionary for logging.  The dictionary
    is of field/value pairs and includes things like
    {
    'PROCESS1_CMD':'foo'
    'PROCESS1_%CPU':'85'
    'PROCESS1_%MEM':'0.1'
    'PROCESS1_USR':'jmyers'
    'PROCESS2_CMD':'bar'
    ...}
    
    Optional parameter:
       top=/path/to/top
    """
    global DEBUG
    returnDict = {}
    #HACK ALERT:
    # for some reason, the TERM variable is not
    # set when we run on PBS.  It really shouldn't
    # matter, since we're running in batch mode,
    # but top is complaining.  So we'll sneak
    # around this by defining the TERM
    # variable.
    #N.B.:  This is not the case in top from
    # procps version 3.2.6,
    # but there's an older version running on
    # TeraGrid. Hopefully someday the following
    # line can be commented out!
    os.putenv("TERM", "xterm")
    f = commands.getoutput(top + " -b -n 1")
    #-b = batch mode, noninteractive
    #-n 1 = run just once, then exit

    linesFromTop = f.split('\n') #split by newline

    keyLineNum = -1
    for i in range(20):
        #look for a line that contains words like PID and COMMAND - this is
        # the line above the process list
        #along the trip, look out for lines like
        #   Cpu0 : 11.1% user, 22.2% system, ....
        # and try to use these to get per-CPU info.
        # This will only work if your ~/.toprc looks like mine does!
        # !!! TO DO : *create* ~/.toprc before doing anything !!!
        try:
            lineFromTop = linesFromTop[i].split(None)
            if len(lineFromTop) > 0:
                if len(lineFromTop[0]) > 3:
                    if lineFromTop[0][0:3] == "Cpu" and lineFromTop[1] == ":" :
                        # this is a "CpuX : YY.Y% user, ZZ.Z%..." line.
                        newKey = lineFromTop[0].upper() + "_" + lineFromTop[3][:-1].upper()
                        #newKey should now be like "CPU0_USER" # get rid of that % sign
                        returnDict[newKey] = lineFromTop[2][:-1]
                        newKey = lineFromTop[0].upper() + "_" + lineFromTop[5][:-1].upper()
                        #newKey should now be like "CPU0_SYS"
                        returnDict[newKey] = lineFromTop[4][:-1] # get rid of that % sign
                        #TO DO:   Make this smart enough to iterate until error. For now, this is OK.
            if "PID" in lineFromTop or "pid" in lineFromTop:
                keyLineNum = i
                break
        except IndexError:
            sys.stderr.write("nlogtopd.py: Something went wrong while reading line "
                             + str(i) + " of the following output from the program top: " +  f)
            nlog.log("Error: Input not nearly long enough for TOP", priority=nlog.CRITICAL,
                     PROG="nlogtopd.py",
                     LASTLINE=i, TOPOUTPUT=f)
            exit(-1)
    if keyLineNum == -1: #no such line found! Panic!
        nlog.log("Error: The utility " + top + " on this machine was"
                 + " very different from expected!", priority=nlog.FATAL,
                 PROG="nlogtopd.py")
        exit(-1)
        
    key = linesFromTop[keyLineNum].split(None)
    for j in range(1,4):
        lineFromTop = linesFromTop[keyLineNum + j].split(None)
        for k in range(len(key)):
            if key[k] in ("%CPU", "COMMAND", "CMD", "USER", "USR", "%MEM",
                          "%cpu", "command", "user", "%mem"):
                returnDict['PROCESS' + str(j) + '_' +
                           key[k]] = lineFromTop[k]

    totalCpuUse = 0.0
    for k in returnDict.keys():
        if len(k) > 3:
            if k[:3] == "CPU" and k[3] in range(10) \
            and k[4] == "_" and k[5:] in ("USER", "SYSTEM", "IDLE", "WAIT"): 
                totalCpuUse += float(returnDict[k])
    
    #returnDict['TOTAL_%CPU'] = getCpuUsage()
    if totalCpuUse != 0.0:
        returnDict['TOTAL_%CPU'] = totalCpuUse
    
    #scuDict = getSpecificCpuUsage()
    #if scuDict != None:
    #    returnDict.update(scuDict)
    
    return returnDict



def Usage():
    sys.stderr.write("nlogtopd.py: This is a Python program intended\n")
    sys.stderr.write(" to write, using nlog.log, a NetLogger-friendly\n")
    sys.stderr.write(" version of information from the utilities top and\n")
    sys.stderr.write(" vmstat regarding CPU usage, etc.  It takes at most\n")
    sys.stderr.write(" one parameter, which must be a number, and represents\n")
    sys.stderr.write(" the frequency with which to write output.\n")




def main(argv=None):    
    #we might want to get this from the policy file eventually
    global TopFrequency

    if len(argv) > 2:
        Usage()
        sys.exit(-1)
    elif len(argv) == 2:
        try:
            TopFrequency  = float(argv[1])
        except:
            Usage()
            sys.exit(-1)

    #write to the log
    while 1:
        # get top processes, cpu usage,
        # maybe even disk i/o stats
        # using top and vmstat...
        # then write them to the local log
        # then sleep for interval seconds
        dataToReport = topProcessesDict()
        nlog.log('TOP',
                 priority=nlog.DEBUG,
                 fieldDict=dataToReport)
        time.sleep(TopFrequency)




if __name__ == '__main__':
    #daemonize!
    # actually, right now, lets leave this as a non-daemon
    # program - it's working better in PBS scripts that way,
    # but eventually we'll probably want it to be a proper
    # daemon.
    #    if os.fork()==0:
    #        os.setsid()
    #        sys.stdout=open("/dev/null", 'w')
    #        sys.stderr=open("/dev/null", 'w')
    #        sys.stdin=open("/dev/null", 'r')
    #        if os.fork()==0:
    main(sys.argv)
    exit(0)



