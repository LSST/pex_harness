#!/usr/bin/env python

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

# -*- python -*-


import lsst.pex.harness.stage as harnessStage

import lsst.pex.harness.Utils
import lsst.pex.logging
import lsst.daf.persistence
import os
from lsst.pex.logging import Log


class SymLinkStageSerial(harnessStage.SerialProcessing):

    """
    SymLinkStage provides a generic mechanism for symbolically linking files
    or directories based on path templates from a Policy and data from the
    clipboard or stage information. SymLinkStageSerial creates the symbolic links
    within the preprocess or postprocess method as designated by the stage policy. 
    """

    def preprocess(self, clipboard):
        """
        Perform the link in the master process.  Desirable if the link is to
        be made once per pipeline execution.
        """
        if self.policy.exists('RunMode') and \
            self.policy.getString('RunMode') == 'preprocess':
            self._link(clipboard)
        
        
    def postprocess(self, clipboard):
        """
        Perform the link in the master process.  Functionally equivalent to
        performing it in preprocess().
        """
        if self.policy.exists('RunMode') and \
            self.policy.getString('RunMode') == 'postprocess':
            self._link(clipboard)

 
    def _link(self, clipboard):
        """
        Link one or more sourcePaths (from policy) to destPaths after
        formatting each with additionalData derived from the clipboard and
        stage information.
        """
        if not self.policy.exists('Links'):
            mylog = Log(Log.defaultLog(), "pex.harness.SymLinkStage.SymLinkStageSerial")
            mylog.log(Log.WARN, "No Links found")
            return

        additionalData = lsst.pex.harness.Utils.createAdditionalData(self, \
                    self.policy, clipboard)

        linkPolicyList = self.policy.getPolicyArray('Links')
        for linkPolicy in linkPolicyList:
            sourcePath = lsst.daf.persistence.LogicalLocation(
                    linkPolicy.getString('sourcePath'), additionalData
                    ).locString()
            destPath = lsst.daf.persistence.LogicalLocation(
                    linkPolicy.getString('destPath'), additionalData
                    ).locString()
            lsst.pex.logging.Trace("pex.harness.SymLinkStage.SymLinkStageSerial", 3, \
                    "linking %s to %s" % (sourcePath, destPath))
            parentDir = os.path.dirname(destPath)
            if parentDir and not os.path.exists(parentDir):
                os.makedirs(parentDir)
            try:
                os.symlink(sourcePath, destPath)
            except OSError, e:
                # ignore "file exists" but re-raise anything else
                if e.errno != 17:
                    raise e

class SymLinkStageParallel(harnessStage.ParallelProcessing):

    """
    SymLinkStage provides a generic mechanism for symbolically linking files
    or directories based on path templates from a Policy and data from the
    clipboard or stage information. SymLinkStageParallel creates the symbolic links
    within a Slice process method if the stage policy so dictates. 
    """

    def process(self, clipboard):
        """
        Perform the link in the slice processes.  Necessary if the link to be
        made depends on the slice number.
        """
        if self.policy.exists('RunMode') and \
        (self.policy.getString('RunMode') == 'preprocess' or \
        self.policy.getString('RunMode') == 'postprocess'):
            return
        self._link(clipboard)

    def _link(self, clipboard):
        """
        Link one or more sourcePaths (from policy) to destPaths after
        formatting each with additionalData derived from the clipboard and
        stage information.
        """
        if not self.policy.exists('Links'):
            mylog = Log(Log.defaultLog(), "pex.harness.SymLinkStage.SymLinkStageParallel")
            mylog.log(Log.WARN, "No Links found")
            return

        additionalData = lsst.pex.harness.Utils.createAdditionalData(self, \
                    self.policy, clipboard)

        linkPolicyList = self.policy.getPolicyArray('Links')
        for linkPolicy in linkPolicyList:
            sourcePath = lsst.daf.persistence.LogicalLocation(
                    linkPolicy.getString('sourcePath'), additionalData
                    ).locString()
            destPath = lsst.daf.persistence.LogicalLocation(
                    linkPolicy.getString('destPath'), additionalData
                    ).locString()
            lsst.pex.logging.Trace("pex.harness.SymLinkStage.SymLinkStageParallel", 3, \
                    "linking %s to %s" % (sourcePath, destPath))
            parentDir = os.path.dirname(destPath)
            if parentDir and not os.path.exists(parentDir):
                os.makedirs(parentDir)
            try:
                os.symlink(sourcePath, destPath)
            except OSError, e:
                # ignore "file exists" but re-raise anything else
                if e.errno != 17:
                    raise e

