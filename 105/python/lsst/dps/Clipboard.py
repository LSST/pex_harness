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

    #------------------------------------------------------------------------
    def __del__ (self):
        """
        Delete the Clipboard 
        """
        print 'Clipboard being deleted'

    #------------------------------------------------------------------------
    def getKeys (self):
        """
        Returns the keys of the python dictionary (in the form of a python 
        list)
        """
        return self.dict.keys()

    #------------------------------------------------------------------------
    def get (self, key):
        """
        Return the value within the dictionary that corresponds to the 
        provided key 
        """
        return self.dict[key]

    #------------------------------------------------------------------------
    def put (self, key, value):
        """
        Add an entry to the dictionary using the provided name/value pair 
        """
        self.dict[key] = value


