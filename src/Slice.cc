// -*- lsst-c++ -*-
/** \file Slice.cc
  *
  * \ingroup harness
  *
  * \brief   Slice represents a single parallel worker program.
  *
  *          Slice executes the loop of Stages for processing a portion of an Image (e.g.,
  *          single ccd or amplifier). The processing is synchonized with serial processing
  *          in the main Pipeline via MPI communications.
  *
  * \author  Greg Daues, NCSA
  */


#include "lsst/pex/harness/Slice.h"
#include <lsst/pex/policy/Policy.h>

namespace dafBase = lsst::daf::base;
namespace pexPolicy = lsst::pex::policy;

class gps_position
{
private:
    friend class boost::serialization::access;

    template<class Archive>
    void serialize(Archive & ar, const unsigned int version)
    {
        ar & degrees;
    }

    string degrees;

public:
    gps_position(){};
    gps_position(string d) : degrees(d)
    {};

};


Slice::Slice() {
}

Slice::~Slice() {
}


Log Slice::initializeLogger(Log defaultLog,  bool isLocalLogMode) {

    _pid = getpid();
    char* _host = getenv("HOST");

    /*  Currentway
    */ 
    sliceLog = defaultLog;  

    /* Newway
    sliceLog = Log::getDefaultLog();
    */ 

    /* EventLog::createDefaultLog(_runId,  _rank, _host, Log::INFO, &preamble); */ 
 

    if(isLocalLogMode) { 
        /* Make a log file name coded to the rank    */ 
        std::stringstream logfileBuffer;
        std::string logfile;

        logfileBuffer << "Slice";
        logfileBuffer << _rank;
        logfileBuffer << ".log";

        logfileBuffer >> logfile;

        /* Make output file stream   */ 
        /* ofstream outlog(logfile.c_str()); */ 
        outlog =  new ofstream(logfile.c_str());

        /* if(!outlog) { cout << "Cannot open file.\n"; return 1; }    */ 

        /* Make LogDestination : LogDestination filedest(&outlog, brief);    */ 

        boost::shared_ptr<LogFormatter> brief(new BriefFormatter(true));
        boost::shared_ptr<LogDestination> tempPtr(new LogDestination(outlog, brief));
        destPtr = tempPtr;
        sliceLog.addDestination(destPtr);  
    } 

    /* filedest.write(lr1);  
    sliceLog.log(Log::INFO, "I'm writing a message.");
    */ 

    Log localLog(sliceLog, "Slice.initializeLogger()");       // localLog: a child log

    localLog.log(Log::INFO,
        boost::format("Logger Initialized : _rank %d ") % _rank);

    string* s1 = new string("propertysetTest"); 
    PropertySet ps3;
    ps3.set("keya", string("TestValue"));

    localLog.log(Log::INFO, *s1, ps3 );


    /* sliceLog.addDestination(outlog, Log::INFO, brief); 
     sliceLog.addDestination(outlog, Log::INFO);  */ 

    return sliceLog;
}

void Slice::initializeMPI() {

    mpiError = MPI_Init(NULL, NULL);  
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }

    mpiError = MPI_Comm_get_parent(&sliceIntercomm);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }

    if (sliceIntercomm == MPI_COMM_NULL) {
        MPI_Finalize();
        exit(1);
    }

    int intercommsize;
    int intercommrank;

    mpiError = MPI_Comm_remote_size(sliceIntercomm, &intercommsize);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }

    if (intercommsize != 1) {
        MPI_Finalize();
        exit(1);
    }

    mpiError = MPI_Comm_rank(sliceIntercomm, &intercommrank);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }

    _rank = intercommrank;

    int flag;
    int *universeSizep;
    mpiError = MPI_Attr_get(sliceIntercomm, MPI_UNIVERSE_SIZE, &universeSizep, &flag);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }
    universeSize = *universeSizep;

    return;
}

void Slice::configureSlice() {

    bufferSize = 256;
    return;
}

void Slice::initialize() {

    initializeMPI();

    configureSlice();

    return;
}

void Slice::invokeShutdownTest() {

    char shutdownCommand[bufferSize];

    mpiError = MPI_Bcast(shutdownCommand, bufferSize, MPI_CHAR, 0, sliceIntercomm);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }


    if(strcmp(shutdownCommand, "SHUTDOWN")) {
    }
    else {
        shutdown();
    }

}

void Slice::invokeBcast(int iStage) {

    char runCommand[bufferSize];
    int kStage;

    Log localLog(sliceLog, "Slice.invokeBcast()");    
    localLog.log(Log::INFO, boost::format("Invoking Bcast: %d ") % iStage);

    mpiError = MPI_Bcast(runCommand, bufferSize, MPI_CHAR, 0, sliceIntercomm);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }

    mpiError = MPI_Bcast(&kStage, 1, MPI_INT, 0, sliceIntercomm);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }

}

void Slice::invokeBarrier(int iStage) {

    Log localLog(sliceLog, "Slice.invokeBarrier()");    
    localLog.log(Log::INFO, boost::format("Invoking Barrier: %d ") % iStage);

    mpiError = MPI_Barrier(sliceIntercomm);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }

}


void Slice::shutdown() {

    MPI_Finalize();
    exit(0);
}

void Slice::setRank(int rank) {
    _rank = rank;
}

int Slice::getRank() {
    return _rank;
}

int Slice::getUniverseSize() {
    return universeSize;
}

void Slice::setTopology(pexPolicy::Policy::Ptr policy) {
    _topologyPolicy = policy;
}

void Slice::setRunId(char* runId) {
    _runId = runId;
}

char* Slice::getRunId() {
    return _runId;
}

const char* Slice::getNeighbors() {
    std::stringstream ss;

    /* 
    std::list<int>::iterator iter=NULL;
    */
    std::list<int>::iterator iter;
    for(iter = neighborList.begin(); iter != neighborList.end(); )
    {
       ss << (*iter);
       ss << ",";
       iter++;
    }
    ss >> neighborString;
    return neighborString.c_str();
};

void Slice::calculateNeighbors() {

    Log localLog(sliceLog, "Slice.calculateNeighbors()");  

    std::string typeTopology; 
    if (_topologyPolicy->exists("type")) {
        typeTopology = _topologyPolicy->getString("type");  
    }

    localLog.log(Log::INFO,
        boost::format("Checking the topology: %s ") % typeTopology);

    int wrank = world.rank();

    localLog.log(Log::INFO,
        boost::format("Checking the ranks within communicators: sliceIntercomm world  %d  %d ") % _rank % wrank );

    if (typeTopology == "ring") {  
        int commSize, isPeriodic;
        int right_nbr, left_nbr;
        isPeriodic = 1;
        MPI_Comm_size(MPI_COMM_WORLD, &commSize );
        MPI_Cart_create(MPI_COMM_WORLD, 1, &commSize, &isPeriodic, 0, &topologyIntracomm );
        MPI_Cart_shift( topologyIntracomm, 0, 1, &left_nbr, &right_nbr );

        neighborList.push_back(left_nbr);
        neighborList.push_back(right_nbr);
    }   

    if (typeTopology == "focalplane") {  
        int commSize[2], isPeriodic[2];
        int rightx, leftx;
        int righty, lefty;
        isPeriodic[0] = 1;
        isPeriodic[1] = 1;

        commSize[0] = _topologyPolicy->getInt("param1");
        commSize[1] = _topologyPolicy->getInt("param2");

        MPI_Cart_create(MPI_COMM_WORLD, 2, commSize, isPeriodic, 0, &topologyIntracomm );
        MPI_Cart_shift( topologyIntracomm, 0, 1, &leftx, &rightx );
        MPI_Cart_shift( topologyIntracomm, 1, 1, &lefty, &righty );

        neighborList.push_back(leftx);
        neighborList.push_back(rightx);
        neighborList.push_back(lefty);
        neighborList.push_back(righty);

        /* 
        sliceLog->log(Log::INFO, boost::format("calculateNeighbors(): %d leftx %d ") % _rank % leftx);
        sliceLog->log(Log::INFO, boost::format("calculateNeighbors(): %d rightx %d") % _rank % rightx);
        sliceLog->log(Log::INFO, boost::format("calculateNeighbors(): %d lefty %d") % _rank % lefty);
        sliceLog->log(Log::INFO, boost::format("calculateNeighbors(): %d righty %d") % _rank % righty);
        */
    }   

}


/* PropertySet Slice::syncSlices(PropertySet ps0) {  */ 

PropertySet::Ptr Slice::syncSlices(PropertySet::Ptr ps0Ptr) {

    Log localLog(sliceLog, "Slice.syncSlices()");    

    /*
    sliceLog.log(Log::INFO,
        boost::format("Enter C++ syncSlices(): _rank %d") % _rank);
    */ 

    localLog.log(Log::INFO, boost::format("Calling Barrier: %d ") % _rank);

    mpiError = MPI_Barrier(sliceIntercomm);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }

    char syncCommand[bufferSize];

    localLog.log(Log::INFO, boost::format("Calling Bcast: %d ") % _rank);

    mpiError = MPI_Bcast(syncCommand, bufferSize, MPI_CHAR, 0, sliceIntercomm);
    if (mpiError != MPI_SUCCESS){
        MPI_Finalize();
        exit(1);
    }

    /* Sync the Slices here */ 

    /* Loop over the Data Properties that need to be shared */ 
    PropertySet::Ptr retPtr(new dafBase::PropertySet);

    /*   If we are passed PropertySet, then do this: 
    std::vector<std::string> vPs0 = ps0.names();
    */ 

    /* If passed a PropertySet::Ptr then do this */  
    /* lousy name 
    std::vector<std::string> vPs0 = ps0Ptr->names();
    */ 
    std::vector<std::string> psNames = ps0Ptr->names();

    /*  Loop over vector using index 
    for (int i = 0; i < vPs0.size(); i++)
    {}
    */ 

    /* Loop over the  nameSetType (vector?)   old stuff 
    std::set<std::string> dpNames = dpt->findNames("^.");
    */  

    /*  Loop over vector using iterator 
    std::set<std::string>::iterator iterSet;
    for(iterSet = psNames.begin(); iterSet != psNames.end(); )
    */ 

    for (vector<string>::const_iterator iterSet = psNames.begin(); iterSet != psNames.end(); )  
    {
        std::string keyToShare = (*iterSet);

        localLog.log(Log::INFO, boost::format("Using keyToShare: %s ") % keyToShare);

        int count = 0;

        PropertySet::Ptr recvPtr[4];
        mpi::request reqs[8];

        int destSlice;
        std::list<int>::iterator iter;
        for(iter = neighborList.begin(); iter != neighborList.end(); iter++)
        {
            destSlice = (*iter);

            localLog.log(Log::INFO, boost::format("Communicating value to Slice %d ") % destSlice);

            reqs[count] = world.isend(destSlice, 0, ps0Ptr);

            count++;
        }

        localLog.log(Log::INFO, boost::format("After isends: %d ") % _rank);


        int srcSlice;
        for(iter = neighborList.begin(); iter != neighborList.end(); iter++)
        {
            srcSlice = (*iter);

            /* perform Recvs */
            localLog.log(Log::INFO, boost::format("Before recv call from Slice %d ") % srcSlice);
            reqs[count] = world.irecv(srcSlice, 0, recvPtr[count-4]);
            localLog.log(Log::INFO, boost::format("After recv from  Slice %d count %d ") % srcSlice %  count);

            count++;

       }

       mpi::wait_all(reqs, reqs + 8);

       localLog.log(Log::INFO, boost::format("Past wait_all %d ") % srcSlice);

       int yy; 
       for (yy = 0; yy < 4; yy++) {
           /* 
           localLog.log(Log::INFO, boost::format("Past wait_all %d yy %d str2 %s ") % _rank % yy % str2[yy]);
           int ival = boost::any_cast<int>(recvPtr[yy]->getValue());
           */ 
           int ival = recvPtr[yy]->get<int>(keyToShare); 

           localLog.log(Log::INFO, boost::format("Past wait_all %d yy %d ival %d  ") % _rank % yy % ival);
           std::stringstream newkeyBuffer;
           std::string newkey;

           newkeyBuffer << keyToShare;
           newkeyBuffer << "-";
           newkeyBuffer << yy;

           newkeyBuffer >> newkey;

           retPtr->set<int>(newkey, ival);
       }

       mpiError = MPI_Barrier(sliceIntercomm);
       if (mpiError != MPI_SUCCESS){
           MPI_Finalize();
           exit(1);
       }
      
       iterSet++;
    }

    return retPtr;

}


