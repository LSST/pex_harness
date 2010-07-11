#! /usr/bin/env python

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


"""
Queue provides the interfaces for a Stage to 
1) obtain the top Clipboard container that will provide it with 
the next image it should operate on (via the InputQueue interface) and 
2) post a Clipboard container that has been updated with 
the image that the present Stage has completed work on for the 
next Stage (via the OutputQueue interface)
"""

class Queue(object):
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
	# print 'Queue being deleted'
        pass

    #------------------------------------------------------------------------
    def getNextDataset(self): 
        """
        Return the Clipboard at the top of the dataset list, removing 
        the Clipboard from the dataset list in the process (pop).
        This method comprises the InputQueue interface
        """
        if not self.datasetList:
            # Code here for the case where the list is empty
            clipboard = None
        else:
            # Code here for the case where the list is NOT empty
            clipboard = self.datasetList.pop(0)

        return clipboard

    #------------------------------------------------------------------------
    def element(self): 
        """
        Return the Clipboard at the top of the dataset list, but do not remove 
        the Clipboard from the dataset list
        """
        if not self.datasetList:
            # Code here for the case where the list is empty
            clipboard = None
        else:
            # Code here for the case where the list is NOT empty
            clipboard = self.datasetList[0]

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

