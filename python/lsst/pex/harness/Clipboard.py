#! /usr/bin/env python

"""
Clipboard provides a container for carrying image data from one Stage of 
the Pipeline to the next.  It wraps a Python dictionary. An image is 
accessed via a get() method where a suitable key (e.g., "primary_image")
must be provided.
"""

class Clipboard:
    '''Container for images: maintains Python dictionary'''

    #------------------------------------------------------------------------
    def __init__ (self):
        """
        Initialize the Clipboard by defining an initial dictionary
        """
        self.dict = {}
        self.isShared = {}

    #------------------------------------------------------------------------
    def __del__ (self):
        """
        Delete the Clipboard 
        """
        # print 'Clipboard being deleted'
        self.close()

    #------------------------------------------------------------------------
    def close(self):
        # print 'Clearing Clipboard dictionary'
        self.dict.clear()
 
    #------------------------------------------------------------------------
    def getKeys (self):
        """
        Returns the keys of the python dictionary (in the form of a python 
        list)
        """
        return self.dict.keys()

    #------------------------------------------------------------------------
    def getSharedKeys (self):
        """
        Returns the shared keys of the python dictionary (in the form of a python 
        list)
        """
        fullKeySet =  self.dict.keys()
        sharedKeySet = []
        for key in fullKeySet:
            if (self.isShared[key] == True):
                sharedKeySet.append(key) 
        return sharedKeySet

    #------------------------------------------------------------------------
    def getItem (self, key):
        """
        Return the value within the dictionary that corresponds to the 
        provided key 
        """
        ##return self.dict[key]
        return self.dict.__getitem__(key)

    #------------------------------------------------------------------------
    def get (self, key, defValue=None):
        """
        Return the value within the dictionary that corresponds to the 
        provided key 
        """
        return self.dict.get(key, defValue)

    #------------------------------------------------------------------------
    def put (self, key, value, isShareable=False):
        """
        Add an entry to the dictionary using the provided name/value pair 
        Set the shared value as well if provided 
        """
        self.dict[key]     = value
        self.isShared[key] = isShareable

    #------------------------------------------------------------------------
    def setShared (self, key, isShareable):
        """
        Set the shared value for this key 
        """
        self.isShared[key] = isShareable

    #------------------------------------------------------------------------
    def contains (self, key):
        """
        Return the value True if the dictionary has a key "key";
        otherwise return False.
        """
        if key in self.dict:
            return True
        else:
            return False

