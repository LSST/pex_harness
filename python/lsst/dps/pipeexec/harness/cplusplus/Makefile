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
CC = mpicc -g 
#for LAM C++
CXX = mpic++ -g 
#for MPICH C++
#CXX = mpicxx -g 

#Note:  external variable NLHOME must be set - for testing, install NetLogger
# yourself or try running from /home/jmyers/NetLogger-3.3.11/ on ds30.ncsa

NLINCLUDES=-I $(NLHOME)/include/ 
LAMINCLUDES=-I/lsst_ibrix/DC1/LAM-7.1.2/include
MPICH1INCLUDES=-I/usr/local/mpich/mpich-p4-1.2.5.2-intel-r2/include -DMPICH
MPICH2INCLUDES=-I/usr/projects/LSST/mpich2-1.0.3/include -DMPICH

DEST  = ${HOME}/bin

# LAM MPI2
#LAMLIBS  = -lmpi  -llam -llammpio -lpthread 
LAMLIBS  = -lmpi  -llammpi++ -llammpio -lpthread 

# MPICH MPI2
MPICH2LIBS  = -L/usr/projects/LSST/mpich2-1.0.3/lib -lmpich -lpthread -lg2c

#NetLogger
NLLIBS=-L  $(NLHOME)/lib/ -lnllite

LSST_INCLUDES=-I../

#for LAM
INCLUDES=$(NLINCLUDES) $(LAMINCLUDES) $(LSST_INCLUDES)
LIBS=$(LAMLIBS) $(NLLIBS)
#For MPICH2
#INCLUDES=$(NLINCLUDES) $(MPICH2INCLUDES) $(LSST_INCLUDES)
#LIBS= $(MPICH2LIBS) $(NLLIBS) 


COPTS=-Wall

CXXINCLUDES=$(INCLUDES)
CXXLIBS=$(LIBS)
CXXOPTS=$(COPTS)

all: all-cpp

#############################
#  C++ Versions
#############################

all-cpp: Manager practice

Manager: Manager.o
	$(CXX) $(CXXLIBS) $(CXXOPTS) Manager.o -o Manager 

Manager.o: Manager.cpp Manager.hpp LSSTException.hpp
	$(CXX)  $(CXXINCLUDES) $(CXXOPTS) -c Manager.cpp  

practice: practice.o
	$(CXX) $(CXXLIBS) $(CXXOPTS) practice.o -o practice 

practice.o: practice.cpp 
	$(CXX)  $(CXXINCLUDES) $(CXXOPTS) -c practice.cpp  


