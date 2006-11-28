#!/usr/bin/env python

import sys
import lsst.mw.dps.pipeexec.queue

USAGE = "Usage: "+ sys.argv[0]+" filename directory"

def main():
	try:
		putfile = sys.argv[1]
	except IndexError:
		print USAGE
		return
	try:
		putdir = sys.argv[2]
	except IndexError:
		print USAGE
		return

	lsst.mw.pipe.queue.put(putfile, putdir)

if __name__ == "__main__":
	main()
