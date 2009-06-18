#! /usr/bin/env python

"""
DataMapper is used by Pipeline to assign data to compute nodes, based
on information about the nodes, assumed to be reported by the nodes
themselves.  Its configuratino is in the policy file passed to the
Pipeline, in a section marked 'dataMapper'.

DataMapper is an abstract superclass, intended to be inherited by
actual implementations, which will implement map().

Specifically, it maps node info, in the form of a PropertySet, to a
data ID, which is also a PropertySet, which typically contains keys
and values which can be substituted into a data path template.  For
example, the node information may be:

    hostname: compute32.lsstcorp.org
    cpus: 16
    cpufamily: xeon 8935
    ram: 32g

And a typical data ID may be:

    ccdId: 14
    ampId: 2

And a typical data path template may be:

    /nfs/visit3382/{%ccdId}/{%ampId}/raw_exposure.fits
"""

class DataMapper:
    '''Assigns data to compute nodes, based on information about the nodes.'''

    def __init__(self):
        pass

    def __del__(self):
        pass

    def map(self, nodeInfo):
        """
        Map nodeInfo (a PropertySet) to a data ID (also a
        propertySet), ready to be key-value substituted into a data
        path.
        """
