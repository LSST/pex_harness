#! /usr/bin/env python

import threading
import lsst.daf.base as dafBase
from lsst.daf.base import *

import lsst.ctrl.events as events
import time

if __name__ == "__main__":
    print "starting...\n"
    externalEventTransmitter = events.EventTransmitter("lsst8.ncsa.uiuc.edu", "triggerImageprocEvent")


    root = PropertySet()
  
    root.set("exposureid", "871034")
    root.set("ccdid", "44")

    externalEventTransmitter.publish(root)
