#! /usr/bin/env python

import threading
import lsst.daf.base as dafBase
from lsst.daf.base import *

import lsst.ctrl.events as events
import time

if __name__ == "__main__":
    print "starting...\n"

    eventBrokerHost = "lsst8.ncsa.uiuc.edu"

    eventRec = events.EventReceiver(eventBrokerHost, "outgoingEvent")

    print 'Python listenEvents : waiting on receive...\n'
    inputParamPropertySetPtr = eventRec.receive(800000)

    print 'Python listenEvents : - received event.\n'

    nameList = inputParamPropertySetPtr.names()
 
    for name in nameList:
        print 'name is ' + name

