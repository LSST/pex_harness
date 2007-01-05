# Make the C/C++ compile portion of a Python extension module
#
# The Makefile.global contains rules for building .c, .o files and sets 
# compile and library flags.  This include requires that make be called 
# with -I option.  
#
include Makefile.global

# These variables are legacy from DC1.  This makefile is not set up to 
# support MPICH at this time.  Support for LAM comes through EUPS and the
# above include file; these are not used.
MPICH1INCLUDES=-I/usr/local/mpich/mpich-p4-1.2.5.2-intel-r2/include -DMPICH
MPICH2INCLUDES=-I/usr/projects/LSST/mpich2-1.0.3/include -DMPICH
MPICH2LIBS  = -L/usr/projects/LSST/mpich2-1.0.3/lib -lmpich -lpthread -lg2c

INSTALLPROGS = 
PROGS = $(INSTALLPROGS) Manager practice

# Make this empty if we don't need to define sighandler_t explicitly
#
# NEED_SIGNALHANDLER_DEF ?= 
NEED_SIGNALHANDLER_DEF ?= -DNEED_SIGNALHANDLER_DEF

CFLAGS += -I.. $(NEED_SIGNALHANDLER_DEF)
CXX = mpic++ -g
CC = mpicc -g

.PHONY : all
all : build

.PHONY : build
build : $(PROGS)

Manager: Manager.o
	$(CXX) $(SUPPORT_LIBS) Manager.o -o Manager 

practice: practice.o
	$(CXX) $(SUPPORT_LIBS) practice.o -o practice 



install : build
#	install -d $(BINDIR)
#	install $(INSTALLPROGS) $(BINDIR)


clean :
	$(RM) *.o _*.{so,dylib} *_wrap.c
	$(RM) core core.[0-9]*[0-9] *~

distclean: clean
	@ $(RM) .Makefile.depend
	$(RM) *.so *.pyc 
	$(RM) $(PROGS)
