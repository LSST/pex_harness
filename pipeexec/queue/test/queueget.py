#!/usr/bin/env python

import os
import sys
import lsst.dps.pipeexec.queue

USAGE = "Usage: "+sys.argv[0]+" directory"

def main():
	try:
		getdir = sys.argv[1]
	except IndexError:
		print USAGE
		return

	while 1:
		filename = lsst.pipe.queue.get(getdir)
		print "got "+filename
		os.unlink(filename)

if __name__ == "__main__":
	main()
