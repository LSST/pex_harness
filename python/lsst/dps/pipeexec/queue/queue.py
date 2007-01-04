#
# Queue
#
import os
import sys
import time

#
# put - moves a file it into a new directory
#
def put(filename, dir):
	file = os.path.basename(filename)
	newdir = dir
	newname = os.path.join(newdir,file)
	os.rename(filename, os.path.join(newdir,file))

#
# get - gets the oldest file in this directory.  If there are no files
#       in this directory, block until a file appears
#
def get(dir):
	while 1:
		names = os.listdir(dir)
		first_time = -1
		first_name = ''
		if (len(names) > 0):
			for i in names:
				cur_file = os.path.join(dir,i)
				cur_time = os.stat(cur_file).st_mtime
				if (first_time == -1):
					first_time = cur_time
					first_name = cur_file
				elif (first_time > cur_time):
					first_time = cur_time
					first_name = cur_file
			return first_name
		time.sleep(1)
