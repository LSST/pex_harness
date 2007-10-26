#! /usr/bin/env python

"""
Test Application Stage 2
"""

from Stage import Stage

class IPDPStage(Stage):

    #------------------------------------------------------------------------
    def process(self):
        """
        Execute the needed processing code for this Stage
        """
        print 'Python IPDPStage process : _rank %i stageId %d' % (self._rank, self.stageId)
        self.activeClipboard = self.inputQueue.getNextDataset()

        inputImage = self.activeClipboard.get("inputImage"); 

        # inputImageDataPropertyPtrType = lsst.mwi.data.DataPropertyPtrType(inputImage)
        print 'Python IPDPStage process(): _rank %i stageId %i value %s' % (self._rank, self.stageId, inputImage)

        self.outputQueue.addDataset(self.activeClipboard)


