# Make the C/C++ compile portion of a Python extension module
#
# The Makefile.global contains rules for building .c, .o files and sets 
# compile and library flags.  This include requires that make be called 
# with -I option.  
#
include Makefile.global

# These variables are legacy from DC1.  This makefile is not set up to 
# support MPICH at this time.  Support for LAM comes through EUPS and the
# above include file
MPICH1INCLUDES=-I/usr/local/mpich/mpich-p4-1.2.5.2-intel-r2/include -DMPICH
MPICH2INCLUDES=-I/usr/projects/LSST/mpich2-1.0.3/include -DMPICH

INSTALLPROGS = manager stageleader stageslice 
PROGS = $(INSTALLPROGS) runOldest 

# Make this empty if we don't need to define sighandler_t explicitly
#
# NEED_SIGNALHANDLER_DEF ?= 
NEED_SIGNALHANDLER_DEF ?= -DNEED_SIGNALHANDLER_DEF

CFLAGS += $(NEED_SIGNALHANDLER_DEF)
CC = mpicc -g

.PHONY : all
all : build

.PHONY : build
build : $(PROGS)


testing: testing.o
	$(CC) testing.o -o testing $(SUPPORT_LIBS)

manager: manager.o
	$(CC) manager.o -o manager $(SUPPORT_LIBS)

stageleader: stageleader.o getOldest.o
	$(CC) stageleader.o getOldest.o -o stageleader $(SUPPORT_LIBS)

stageslice: stageslice.o
	$(CC) stageslice.o -o stageslice $(SUPPORT_LIBS)

runOldest: runOldest.o getOldest.o
	$(CC) runOldest.o getOldest.o -o runOldest $(SUPPORT_LIBS)

testMPI:  testMPI.o
	$(CC) testMPI.o -o testMPI $(SUPPORT_LIBS)

writetodummyfile:	writetodummyfile.o
	$(CC) writetodummyfile.o -o writetodummyfile


install : build
	install -d $(BINDIR)
	install $(INSTALLPROGS) $(BINDIR)


clean :
	$(RM) *.o _*.{so,dylib} *_wrap.c
	$(RM) core core.[0-9]*[0-9] *~

distclean: clean
	@ $(RM) .Makefile.depend
	$(RM) *.so *.pyc wcstools.py*
	$(RM) $(PROGS)
