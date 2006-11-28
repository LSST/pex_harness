#!/usr/bin/env python
"""Installer for harness application
including support files in lsst.mw.dps.pipeexec.harness

"""
import sys
from lsst.apps.support import setuputil

# get version
currSysPath = sys.path
sys.path = ["python"] + list(currSysPath)
sys.path = currSysPath

setuputil.stdSetup(
	name = "lsst.mw.dps.pipeexec.harness",
	description = "LSST pipeline harness",
)
