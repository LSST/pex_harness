#! /usr/bin/env python

"""
Test Application Stage 4
"""

from Stage import Stage

class App4Stage(Stage):

    #------------------------------------------------------------------------
    def preprocess(self): 
        """
        Execute the needed preprocessing code for this Stage
        This method will be overwritten by the Stage subclass
        """
	print 'Python App4Stage preprocess : stageId %d' % self.stageId
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()
        for key in keys:
            value = self.activeClipboard.get(key)
	    print 'Python App4Stage preprocess(): stageId %i key %s value %s' % (self.stageId, key, value)

    #------------------------------------------------------------------------
    def process(self):
        """
        Execute the needed processing code for this Stage
        """
        print 'Python App4Stage process : _rank %i stageId %d' % (self._rank, self.stageId)
        self.activeClipboard = self.inputQueue.getNextDataset()
        keys = self.activeClipboard.getKeys()
        for key in keys:
            value = self.activeClipboard.get(key)
            print 'Python App4Stage process(): _rank %i stageId %i key %s value %s' % (self._rank, self.stageId, key, value)

        self.outputQueue.addDataset(self.activeClipboard)

    #------------------------------------------------------------------------
    def postprocess(self): 
        """
        Execute the needed postprocessing code for this Stage
        This method will be overwritten by the Stage subclass
        """
	print 'Python App4Stage postprocess : stageId %d' % self.stageId
        self.outputQueue.addDataset(self.activeClipboard)


