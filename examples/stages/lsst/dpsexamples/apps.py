#! /usr/bin/env python

"""
Test Application Stages
"""

from lsst.pex.harness.Stage import Stage

import lsst.daf.base as datap

class App1Stage(Stage):

    #------------------------------------------------------------------------
    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        """
	print 'Python apps.App1Stage preprocess : stageId %i' % self.stageId
	print 'Python apps.App1Stage preprocess : universeSize %i' % self._universeSize

        self.activeClipboard = self.inputQueue.getNextDataset()

        root = datap.DataProperty.createPropertyNode("root");

        visitId  = datap.DataProperty("visitId", 1)
        FOVRa    = datap.DataProperty("FOVRa", 273.48066298343)
        FOVDec   = datap.DataProperty("FOVDec", -27.125)

        root.addProperty(visitId)
        root.addProperty(FOVRa)
        root.addProperty(FOVDec)

        self.activeClipboard.put("outgoingEvent", root)


    #------------------------------------------------------------------------
    def postprocess(self): 
        """
        Execute the needed postprocessing code for this Stage
        """
	print 'Python apps.App1Stage postprocess : stageId %d' % self.stageId
        self.outputQueue.addDataset(self.activeClipboard)



