#! /usr/bin/env python

import threading
import lsst.daf.base as dafBase
from lsst.daf.base import *

import lsst.ctrl.events as events
import time

if __name__ == "__main__":

    print "starting...\n"

    eventBrokerHost = "lsst8.ncsa.uiuc.edu" 

    externalEventTransmitter = events.EventTransmitter(eventBrokerHost, "triggerAssociationEvent")

    root = PropertySet()

    root.setInt("visitId", 1)
    root.setDouble("FOVRa", 273.48066298343)
    root.setDouble("FOVDec", -27.125)

    externalEventTransmitter.publish(root)

