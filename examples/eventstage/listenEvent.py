#! /usr/bin/env python

import lsst.daf.base as datap
import lsst.ctrl.events as events
import time

if __name__ == "__main__":
    print "starting...\n"

    activemqBroker = "lsst8.ncsa.uiuc.edu"

    eventRec = events.EventReceiver(activemqBroker, "outgoingEvent")

    print 'Python listenEvents : waiting on receive...\n'
    inputParamPropertyPtrType = eventRec.receive(800000)
    print 'Python listenEvents : - received event.\n'
