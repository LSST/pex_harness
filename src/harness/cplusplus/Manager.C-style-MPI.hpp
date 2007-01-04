// jmyers - oct. 2006

// C++ Harness Notes:

// For now, I'm not going to try to "OOP"-ize the MPI handling.
// I think that doing so would be either a hack or way beyond
// the scope of this project - MPI and OOP just don't get along
// very well, in my opinion (at least, not yet).

// I think the right way to do things is to put per-process code
// in int main (e.g., MPI_Init, MPI_Finalize, signal handling, logger creation)
// and let the C++ classes freely use MPI calls.  I don't think that
// any methods for doing MPI wrapper classes will be very useful (I 
// wrote for a while in my notes exploring several plans, mostly based
// on singleton models, but they only make the code bulkier, uglier, and
// still require collusion on the part of the user).  I think it's fair
// to keep things this way - though in the UML we see classes that are basically
// MPI functions paired with an MPI_Intercomm, which might be a slightly
// useful abstraction.

// Also, I'm just going to leave the logging facilities as they were for now.
// the NetLogger C interface is all done with macros, and everything is global.  This
// is again rather un-OOP, but myself and Robyn both liked the interface
// that we had.  Since MPI dictates that we need to keep our entities
// in separate executables anyway, this seems to be a realistic approach.  
// Furthermore, since we choose logfile names based on PID and username, 
// it seems like this belongs with "process-level" data, not "class-level" 
// data.

// It will make debugging easier for now, but 
// like MPI, it might be good to someday make a more cohesive wrapper
// around the logs - perhaps even matching our Python version!  However
// this won't be entirely useful until we have a C-interface parser for
// the policy files that hold information like the name of the log file (!)
// but once we get there, there will have to be a slightly different approach
// taken to the handling of log files here.


// We might also try to avoid redundancy eventually and make 
// a standard main_program() that is called by all int main()s in all Pipeline
// processes, since they all look a lot alike.
// For now I think that might get slightly too hairy.

// TBD:  I noticed that the command-line option PipelineExe is never used.  I'm 
// actually a little vague on what it should be - at first I assumed this was
// the name of the stageleader executable, but the definition is actually a little vague.
// ergo, none of these pipelineExe variables (in the PipelineOptions or PieplineManager classes)
// are actually set - and in startStageLeaders, we just use a static string. TO DO: do the Right Thing!

#ifndef MPICH
#include <lam_config.h> 
#endif

#include <mpi.h> 
#include <stdio.h>
#include <string>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h> /* for getopt  and unlink */
#include <libgen.h> /* for basename */
#include <getopt.h>
#include <iostream>
#include "nl_log.h"
#include "pipelineHarness.h"


#define NOTHIN_BUT_DEBUG 0

using namespace std;


/* helper classes */

class PipelineOptions {
public:
  string policyDir;
  string diskPolicy;
  string pipelinePolicy;
  string pipelineExe;
  string inputQ;
  string numStageStr;
  string nodesList;
  int numSlice;
  int numStage;
};


class PipelineOptionReader {
public: 
  PipelineOptions readOptionsFromCommandLine(int argc, char** argv);
};






/* 
   the actual pipeline manager class
*/



class PipelineManager {
public:
  PipelineManager(PipelineOptions);
  ~PipelineManager();
  void runPipeline();
  void terminatePipeline();
private:

  string getStageExec(int stageNum, string WD);
  void setMPIInfo();
  void spawnStageLeaders();
  void collectStageLeaderPIDs();

  // -------variables------------ //
  
  /* input */
  string PWD;
  string policyDir;
  string diskPolicy;
  string pipelinePolicy;
  string pipelineExe;
  string inputQ;
  string numStageStr;
  string nodesList;
  int numSlice;
  int numStage;


  /* for keeping tack of our stageleaders and killing them*/
  string* stageleaderHostList;
  int* stageleaderPidList;
  int stageleaderPidListLen;
  
  /* data from MPI*/
  int world_size;
  int world_rank;
  MPI_Comm everyone;
  int universe_size;
};



/* helper functions used by int main are not declared here.  This is for class defs only.*/


