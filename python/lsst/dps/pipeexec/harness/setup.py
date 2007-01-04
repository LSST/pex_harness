#!/usr/bin/env python
"""Installer for harness application
including support files in lsst.dps.pipeexec.harness

"""
import sys
from lsst.support import setuputil

# get version
currSysPath = sys.path
sys.path = ["python"] + list(currSysPath)
sys.path = currSysPath

setuputil.stdSetup(
	name = "lsst.dps.pipeexec.harness",
	description = "LSST pipeline harness",
)
