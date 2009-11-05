#! /usr/bin/env python

import threading
import lsst.daf.base as dafBase
from lsst.daf.base import *

import lsst.ctrl.events as events
import time

if __name__ == "__main__":
    print "starting...\n"

    eventBrokerHost = "lsst4.ncsa.uiuc.edu"

    externalEventTransmitter = events.EventTransmitter(eventBrokerHost, "triggerMatchMopEvent")

    root = PropertySet()

    root.setInt("visitId", 1)

    externalEventTransmitter.publish(root)



