from __future__ import with_statement
import sys
import os
import subprocess
import time

import eups
import lsst.mwi.utils

RootIoDir = "/share/DC2root"
Dc2PipeDir = "/share/stack/dc2pipe"
DbHost = "lsst10.ncsa.uiuc.edu "
DbUser = "test"
DbPassword = "globular.test"
DbCmdFiles = ["lsstSchema4mysql.sql", "lsstPipelineSetup4mysql.sql"]

def startPipeline(nodeList, pipelinePolicy, runId, createIoDirs=False, createDbTables=False):
    """Start pipeline execution
    
    Inputs:
    - nodeList: path to mpi machine file; environment variables and relative paths are expanded
      relative policy paths are relative to the directory containing nodeList
    - pipelinePolicy: path to pipeline policy file
    - createIoDirs: create I/O directories for pipeline run
    - createDbTables: create database tables
    
    Set createIoDirs and createDbTables True for DC2 operation, False for running local tests.
    
    The node list file contains information about the nodes on which to run the pipeline.
    In its simplest form there is one line per node in the format:
       ipaddress[:ncores]
    where ncores is the number of cores that you wish to use on that node; the defaults is 1.
    Blank lines and lines beginning with # are ignored.
    Additional options may be specified; see documentation for MPICH2 used with
    the MPD process management environment.

    The pipeline uses one slice just to run the preprocess and postprocess phase;
    all other slices are used to run slices of the process phase.
    For example:
    - To run one slice of the process phase on one host (that has at least 2 CPUs):
      specify one host with 2 slices (1 for pre/postprocess and 1 for process)
    - To run three slices of the process phase on two hosts (each with at least 2 CPUs):
      specify two hosts with 2 slices each (4 slices: 1 for pre/postprocess and 3 for process)
    """
    nodeList = os.path.abspath(os.path.expandvars(nodeList))
    pipelineDir = os.path.dirname(nodeList)
    
    # create I/O directories for this run
    if createIoDirs:
        lsst.mwi.utils.Trace("dps.startPipeline", 3, "Creating I/O directories for run %s" % (runId,))
        runRoot = os.path.join(RootIoDir, runId)
        if os.path.exists(runRoot):
            raise RuntimeError("runId %s already exists" % (runId,))
        os.mkdir(runRoot)
        os.mkdir(os.path.join(runRoot, "ipd"))
        os.mkdir(os.path.join(runRoot, "ipd", "input"))
        os.mkdir(os.path.join(runRoot, "ipd", "output"))
        os.mkdir(os.path.join(runRoot, "mops"))
        os.mkdir(os.path.join(runRoot, "mops", "input"))
        os.mkdir(os.path.join(runRoot, "mops", "output"))
        os.mkdir(os.path.join(runRoot, "assoc"))
        os.mkdir(os.path.join(runRoot, "assoc", "input"))
        os.mkdir(os.path.join(runRoot, "assoc", "update"))
        os.mkdir(os.path.join(runRoot, "assoc", "output"))
    else:
        lsst.mwi.utils.Trace("dps.startPipeline", 3, "Not creating I/O directories for run %s" % (runId,))
    
    # create database tables for this run
    if createDbTables:
        lsst.mwi.utils.Trace("dps.startPipeline", 3, "Creating database tables for run %s" % (runId,))
        dbBaseArgs = ["mysql", "-h", "lsst10.ncsa.uiuc.edu", "-u"+DbUser, "-p"+DbPassword]
        subprocess.call(dbBaseArgs + ["-e", 'create database "%s"' % (runId,)])
        for sqlCmdFile in DbCmdFiles:
            with file(os.path.join(Dc2PipeDir, sqlCmdFile)) as sqlFile:
                subprocess.call(dbBaseArgs + [runId], stdin=sqlFile)
    else:
        lsst.mwi.utils.Trace("dps.startPipeline", 3, "Not creating database tables for run %s" % (runId,))

    lsst.mwi.utils.Trace("lsst.dps.startPipeline", 3, "pipelineDir=%s" % (pipelineDir,))

    nnodes, ncores = parseNodeList(nodeList)
    lsst.mwi.utils.Trace("lsst.dps.startPipeline", 3, "nnodes=%s; ncores=%s; nslices=%s" % \
        (nnodes, ncores, ncores-1))
    
    lsst.mwi.utils.Trace("lsst.dps.startPipeline", 3, "Running mpdboot")
    subprocess.call(["mpdboot", "--totalnum=%s" % (nnodes,), "--file=%s" % (nodeList,), "--verbose"])
    
    time.sleep(3)
    lsst.mwi.utils.Trace("lsst.dps.startPipeline", 3, "Running mpdtrace")
    subprocess.call(["mpdtrace", "-l"])
    time.sleep(2)
    
    lsst.mwi.utils.Trace("lsst.dps.startPipeline", 3, "Running mpiexec")
    subprocess.call(
        ["mpiexec", "-usize", str(ncores), "-machinefile", nodeList, "-n", "1",
        "runPipeline.py", pipelinePolicy, runId],
        cwd = pipelineDir,
    )
    
    time.sleep(1)    
    lsst.mwi.utils.Trace("lsst.dps.startPipeline", 3, "Running mpdallexit")
    subprocess.call(["mpdallexit"])

def parseNodeList(nodeList):
    """Return (nnodes, ncores)
    where:
    - nnodes = number of nodes (host IP addresses) in file
    - ncores = total number of CPU cores specified    
    """
    nnodes = 0
    ncores = 0
    with file(nodeList, "r") as nodeFile:
        for line in nodeFile:
            line = line.strip()
            if not line:
                continue
            if line.startswith("#"):
                continue

            try:
                # strip optional extra arguments
                hostInfo = line.split()[0]
                # parse optional number of slices
                hostSlice = hostInfo.split(":")
                if len(hostSlice) == 1:
                    ncores += 1
                elif len(hostSlice) == 2:
                    ncores += int(hostSlice[1])
                else:
                    raise RuntimeError("Could not parse host info %r" % hostInfo)
            except Exception, e:
                raise RuntimeError("Cannot parse nodeList line %r; error = %s" % (line, e))
            nnodes += 1
    return (nnodes, ncores)
            
            