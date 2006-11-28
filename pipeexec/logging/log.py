#!/usr/bin/env python
"""
Logger

Author: Michelle Miller
Date:	Aug. 3, 2006

Sends log messages to a central log facility using TCP sockets.
"""
__all__ = ["log", "CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]

import logging, logging.handlers
import socket

CRITICAL = "CRIT"
ERROR    = "ERR"
WARNING  = "WARNING"
INFO     = "INFO"
DEBUG    = "DEBUG"

PRIORITY = {
	'CRIT'	 : logging.CRITICAL,
	'ERR'	 : logging.ERROR,
	'WARNING': logging.WARNING,
	'INFO'	 : logging.INFO,
	'DEBUG'	 : logging.DEBUG,
}

def log (message, priority, source=""):
	"""Log a message
	
	Inputs:
	- message: message to log
	- priority: message priority; one of:
	  - CRITICAL
	  - ERROR
	  - WARNING
	  - INFO
	  - DEBUG
	  The meanings are the same as for the syslog unix utility
	- source: an optional tag to use for log sorting.
	  Potential uses for this field are source filename
	  or location in the code that originated the message
	"""
	#print message	#debug only

	rootLogger = logging.getLogger('')
	rootLogger.setLevel(logging.DEBUG)

	# read in destination from a config file - create config command
	# I am precluding other types of logging - extend here
	socketHandler = logging.handlers.SocketHandler('ds30.ncsa.uiuc.edu',
									2151,
	)
	rootLogger.addHandler(socketHandler)

	# add hostname to message stream. Another option - sort by source
	my_machine = socket.gethostname()
	logger1 = logging.getLogger(my_machine)
	#logger1 = logging.getLogger(source)

	final_message = ": ".join((source, message))

	try:
		prior = PRIORITY[priority]
	except:
		raise RuntimeError, "invalid priority"

	logger1.log (prior, final_message)
