#! /usr/bin/env python

import lsst.mwi.data as datap
import lsst.events as events
import time

if __name__ == "__main__":
    print "starting...\n"

    activemqBroker = "lsst4.ncsa.uiuc.edu"

    eventRec = events.EventReceiver(activemqBroker, "outgoingEvent")

    print 'Python listenEvents : waiting on receive...\n'
    inputParamPropertyPtrType = eventRec.receive(800000)
    print 'Python listenEvents : - received event.\n'
