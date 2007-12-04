#! /usr/bin/env python

import lsst.mwi.data as datap
import lsst.events as events
import time

if __name__ == "__main__":
    print "starting...\n"
    eventRec = events.EventReceiver("lsst8.ncsa.uiuc.edu", "outgoingEvent")

    print 'Python listenEvents : waiting on receive...\n'
    inputParamPropertyPtrType = eventRec.receive(800000)
    print 'Python listenEvents : - received event.\n'
