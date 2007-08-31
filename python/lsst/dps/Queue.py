#! /usr/bin/env python

"""
Queue provides the interfaces for a Stage to 
1) obtain the top Clipboard container that will provide it with 
the next image it should operate on (via the InputQueue interface) and 
2) post a Clipboard container that has been updated with 
the image that the present Stage has completed work on for the 
next Stage (via the OutputQueue interface)
"""

class Queue:
    '''Carry references to image data from Stage to Stage via ClipBoard container'''

    #------------------------------------------------------------------------
    def __init__(self):
        """
        Initialize the Queue by defining an initial dataset list
        """
        self.datasetList = []

    #------------------------------------------------------------------------
    def __del__(self):
        """
        Delete the Queue object for cleanup
        """
	print 'Queue being deleted'

    #------------------------------------------------------------------------
    def getNextDataset(self): 
        """
        Return the Clipboard at the top of the dataset list, removing 
        the Clipboard from the dataset list in the process (pop).
        This method comprises the InputQueue interface
        """
        clipboard = self.datasetList.pop(0)
        return clipboard

    #------------------------------------------------------------------------
    def addDataset(self, clipboard): 
        """
        Append the given Clipboard to the dataset list.
        This method comprises the OutputQueue interface
        """
        self.datasetList.append(clipboard)

    #------------------------------------------------------------------------
    def size(self): 
        """
        Return the size of the dataset list (the number of Clipboards)
        """
        size = len(self.datasetList)
        return size 	

