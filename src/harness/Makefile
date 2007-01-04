#
# pipeline makefile
#
#
############################################################################
###########################################################################
#  For TERAGRID NETLOGGER use - if NETLOGGER not PACMAN installed, install 
#				following block in your ~/.tcshrc (or equiv)
#			- once TG PACMAN installed, this block can be removed
#				from this Makefile and your startup script
############################################################################
#
# Temporary setup for NetLogger
#setenv NLHOME /usr/projects/LSST/LSSTProto/NetLogger-3.3.11
#setenv LD_LIBRARY_PATH $NLHOME/lib:${LD_LIBRARY_PATH}
#setenv PYTHONPATH $NLHOME/lib/python2.4/site-packages:$NLHOME/bin:${PYTHONPATH}
#setenv PATH $NLHOME/bin:${PATH}
#
############################################################################
############################################################################
CC = mpicc -g 

#Note:  external variable NLHOME must be set - for testing, install NetLogger
# yourself or try running from /home/jmyers/NetLogger-3.3.11/ on ds30.ncsa

NLINCLUDES=-I $(NLHOME)/include/ 
LAMINCLUDES=-I/lsst_ibrix/DC1/LAM-7.1.2/include
MPICH1INCLUDES=-I/usr/local/mpich/mpich-p4-1.2.5.2-intel-r2/include -DMPICH
MPICH2INCLUDES=-I/usr/projects/LSST/mpich2-1.0.3/include -DMPICH

DEST  = ${HOME}/bin

# LAM MPI2
LAMLIBS  = -lmpi  -llam -llammpio -lpthread

# MPICH MPI2
MPICH2LIBS  = -L/usr/projects/LSST/mpich2-1.0.3/lib -lmpich -lpthread -lg2c

#NetLogger
NLLIBS=-L  $(NLHOME)/lib/ -lnllite

#for LAM
#INCLUDES=$(NLINCLUDES) $(LAMINCLUDES)
#LIBS=$(LAMLIBS) $(NLLIBS)
#For MPICH2
INCLUDES=$(NLINCLUDES) $(MPICH2INCLUDES)
LIBS= $(MPICH2LIBS) $(NLLIBS)
COPTS=-Wall

all: manager stageleader stageslice runOldest


testing: testing.o
	$(CC) testing.o -o testing $(LIBS) $(COPTS)

manager: manager.o
	$(CC) manager.o -o manager $(LIBS) $(COPTS)

stageleader: stageleader.o getOldest.o
	$(CC) stageleader.o getOldest.o -o stageleader $(LIBS) $(COPTS)

stageslice: stageslice.o
	$(CC) stageslice.o -o stageslice $(LIBS) $(COPTS)

runOldest: runOldest.o getOldest.o
	$(CC) runOldest.o getOldest.o -o runOldest $(LIBS) $(COPTS)

testMPI:  testMPI.o
	$(CC) testMPI.o -o testMPI $(LIBS) $(COPTS)

writetodummyfile:	writetodummyfile.o
	$(CC) writetodummyfile.o -o writetodummyfile $(COPTS)

clean:
	$(RM) stageleader manager stageslice *.o 

manager.o:  manager.c 
	$(CC) $(COPTS) $(INCLUDES) -c manager.c

stageleader.o:  stageleader.c 
	$(CC) $(COPTS) $(INCLUDES) -c stageleader.c

stageslice.o:  stageslice.c 
	$(CC) $(COPTS) $(INCLUDES) -c stageslice.c

getOldest.o:  getOldest.c 
	$(CC) $(COPTS)  $(INCLUDES) -c getOldest.c

runOldest.o:  runOldest.c 
	$(CC) $(COPTS)  $(INCLUDES) -c runOldest.c

testMPI.o: testMPI.c
	$(CC) $(COPTS) $(INCLUDES) -c testMPI.c

writetodummyfile.o:	writetodummyfile.c
	$(CC) $(COPTS) $(INCLUDES) -c writetodummyfile.c

getstagecode.o: getstagecode.c
	$(CC) $(COPTS) $(INCLUDES) -c getstagecode.c

getstagecode:	getstagecode.o
	$(CC) getstagecode.o -o getstagecode $(LIBS) $(COPTS)

install: manager stageleader stageslice 
	cp manager $(LSST_DEVEL)/bin
	cp stageleader $(LSST_DEVEL)/bin
	cp stageslice $(LSST_DEVEL)/bin
