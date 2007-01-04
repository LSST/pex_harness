/* stageleader */ 
/* Input args
 * stagenumber is no longer used as a parameter.  Our stage number will be 
 * decided by our MPI Rank.  This will still be a number from 0 to 
 * (numStages - 1) and because of the dynamics of MPI and this harness, 
 * our stagenumber has always
 * happened to be the same as our MPI rank, which is fortunate - otherwise
 * things would have exploded pretty quickly. --jmyers
  
 * argv[1] - NumSlices       - the number of slices 
 * argv[2] - WorkDir         - full path name to working directory that will 
 *			       be used for log files
 * argv[3] - StageWorkerPath - full path name to pipeline executable 
 * argv[4] - policyDir       - name to pipeline policy directory
 * argv[5] - pipelinePolicy  - name of pipeline policy file 
 * argv[6] - diskPolicy      - name of disk locator policy file 
 * argv[7] - inputQ          - name of input queue for stage0
 * argv[8] - nodeList        - full path name to file containing nodes to 
 *			       use for stageslices
 */
 
#ifndef MPICH
#include <lam_config.h>
#endif

#include <mpi.h> 
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include "nl_log.h"
#include "pipelineHarness.h"

#ifndef MAX_COMMAND_SIZE
#define MAX_COMMAND_SIZE 256
#endif

/* for the messages sent with MPI_Ssend and MPI_Recv */
#define KEEP_RUNNING 0
#define LETS_DIE_NOW 1

/*typedef void (*sighandler_t)(int);  no longer needed when GNU 
  extensions are enabled*/

sighandler_t signal(int signum, sighandler_t handler);

time_t time(time_t *t);
char *getenv(const char *name);

#define  STAGE_SYNC_TAG 231

/* define USE_IOQUEUE only if harness should wait for input file availability */
#define USE_IOQUEUE 
int get_oldster();


static int pidList[128];
static char hostList[MAX_NODE_COUNT][128];
static int pidListLen;
static int killed = 0;
static MPI_Status status;

void handleTERM(int signal)
{
  int k;
  char cmd[MAX_COMMAND_SIZE];
  fprintf(stderr,"Stageleader_received_sigTerm.");
  fflush(stderr);
  NL_fatal("err", "Received_SIGTERM,_informing_stageSlices", "", "");
  for (k = 0; k < pidListLen; k++) {
    snprintf(cmd,MAX_COMMAND_SIZE,"ssh %s kill -TERM %d", hostList[k], pidList[k]);
    system(cmd);
    NL_info("dbg", "Killing_all_child processes", "WITH_CMD=sPROG=s",
            cmd, "StageLeader");
  }

  killed = 1;
  exit(0);
}

int main(int argc, char *argv[]) { 
   int size; 
   MPI_Comm managerIntercomm, stageBarrierComm, stageLeaderComm; 
   int myparent;

   int universe_size, *universe_sizep, flag;

   int numStages, lastStage;

   char *nargv[10];
   /* >>>>>>>>>>>>>>>>>>>>>>    BUG    >>>>>>>>>>>>>>>>>>>>>>>>>>
   char **nargv;
   >>>>>>>>>>>>>>>>>>>>>>>>>    BUG    >>>>>>>>>>>>>>>>>>>>>>>>>>*/

   /* copied argv values */
   int stageNum;
   int numSlices;
   char stageNumStr[256];
   char numSlicesStr[256];
   char WD[MAX_PATH_SIZE];          /* working directory for log files, etc */
   char StageWorkerPath[MAX_PATH_SIZE];
   char policyDir[MAX_PATH_SIZE];
   char pipelinePolicy[MAX_PATH_SIZE];
   char diskPolicy[MAX_PATH_SIZE];
   char inputQ[MAX_PATH_SIZE];
   char nodeList[MAX_PATH_SIZE];

   char logfile[MAX_PATH_SIZE];
   char username[MAX_ENV_VAR_SIZE];


   MPI_Comm stageIntercomm;         /* intercommunicator */
   int bogus;                       /* bogus arg needed by LAM/MPI_Gather */

   /* process argv */
   numSlices = atoi(argv[1]);
   strncpy(numSlicesStr, argv[1], 256);
   strncpy(WD, argv[2], MAX_PATH_SIZE);
   strncpy(StageWorkerPath, argv[3], MAX_PATH_SIZE);
   strncpy(policyDir, argv[4], MAX_PATH_SIZE);
   strncpy(pipelinePolicy, argv[5], MAX_PATH_SIZE);
   strncpy(diskPolicy, argv[6], MAX_PATH_SIZE);
   strncpy(inputQ, argv[7], MAX_PATH_SIZE);
   strncpy(nodeList, argv[8], MAX_PATH_SIZE);

   /* set LSST_POLICY_DIR as environment variable */
   if (setenv(LSST_POLICY_DIR,policyDir,1) < 0) {
     fprintf(stderr,"FATAL: unable to set environment var for LSST_POLICY_DIR:%s\n", policyDir);
     fflush(stderr);
     exit(1);
   }

   /* initialize logging now that $LSST_POLICY_DIR is available */
   int mypid;
   mypid = getpid();

   strncpy(username, getenv("USER"),MAX_ENV_VAR_SIZE);
   if (username == NULL)
     {
       fprintf(stderr, "stageleader.c: Got $USER == NULL! Cannot create log file!\n");
       MPI_Finalize();
       exit(1);
     }

   snprintf(logfile, MAX_PATH_SIZE,"/tmp/lsst.harness.stageleader.%i.%s.log", mypid, username);

   NL_logger_module("err1", logfile, NL_LVL_ERROR, NL_TYPE_APP, "");

   if (MPI_Init(&argc, &argv) != MPI_SUCCESS){
     NL_fatal("err1","Failed_MPI_Init", "PROG=s", "StageLeader");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
   }

   /* At this point the MPI_COMM_WORLD intracomm contains all the stageleaders.
    * Make a duplicate to keep its usage clear.
   */

   if (MPI_Comm_dup(MPI_COMM_WORLD, &stageLeaderComm) != MPI_SUCCESS){
     NL_fatal("err1","Failed_MPI_Comm_dup", "PROG=s", "StageLeader");
     NL_logger_del(); /* this should flush all loggers and clear mem*/
     MPI_Finalize();
     exit(1);
   }

   if (MPI_Comm_rank(stageLeaderComm,&stageNum) != MPI_SUCCESS){
     NL_fatal("err1","Failed_MPI_Comm_rank", "PROG=s", "StageLeader");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
   }

   /* now that have good and unique id - switch to uniquely named log file
      so that fault files are not overwritten
   */
   /*   NL_logger_del(); */
   NL_logger_module_const("dbg", logfile,  NL_LVL_INFO,
			  "MYPID:iPROG:sHOST:sSTAGENUM:i", 
			  mypid, "stageleader", ipaddr(), stageNum);
   NL_logger_module_const("err", logfile,   NL_LVL_ERROR, 
			  "MYPID:iPROG:sHOST:sSTAGENUM:i", 
			  mypid, "stageleader", ipaddr(),stageNum);

   NL_info("dbg", "stageleader.main.start", "", "");
   NL_info("dbg", "stageleader.getMPIInfo.start", "", "");

   if (MPI_Comm_size(stageLeaderComm, &numStages) != MPI_SUCCESS){
     NL_fatal("err","Failed_MPI_Comm_size", "", "");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
   }
   lastStage = numStages - 1;

   /* Get intercomm created when manager called MPI_Spawn to start the
    * stageleaders.   This will be used to communicate back to the manager
    */

   if (MPI_Comm_get_parent(&managerIntercomm) != MPI_SUCCESS){
     NL_fatal("err","Failed_MPI_Comm_get_parent","", "");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
   }
   if (managerIntercomm == MPI_COMM_NULL) {
     NL_fatal("err","managerIntercomm==MPI_COMM_NULL", "", "");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
   }
   if (MPI_Comm_remote_size(managerIntercomm, &size) != MPI_SUCCESS){
     NL_fatal("err","stageleader:_Failed MPI_Comm_remote_size5", "", "");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
   }
   if (size != 1) {
     NL_fatal("err","Something_wrong_with_parent", 
	      "REMOTE_SIZE=i", size);
     NL_logger_del();
     MPI_Finalize();
     exit(1);
   }
 
   if (MPI_Comm_rank(managerIntercomm,&myparent) != MPI_SUCCESS){
     NL_fatal("err","Failed_MPI_Comm_rank", "","");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
   }
   /* 
    * The manager is represented as the process with rank 0 in (the remote 
    * group of) MPI_COMM_PARENT.  If the stageleaders need to communicate among 
    * themselves, they can use stageLeaderComm. 
    */ 

   NL_info("dbg", "stageleader.getMPIInfo.end",
           "NUM_STAGES=iMANAGER_INTERCOMM_REMOTE_SIZE=i",
           numStages,size);

   /* Send own PID up to the manager */
   int pid;
   pid = getpid();

   char hostName[128];
   gethostname(hostName, 128);

   NL_info("dbg", "stageleader.MPI_Gather.start", "", "");

   if (MPI_Gather((void *)&pid, 1, MPI_INT, &bogus, 1, MPI_INT, 0, managerIntercomm) != MPI_SUCCESS) {
     NL_fatal("err","Failed_MPI_Gather", "", "");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
   }

   if (MPI_Gather((void *)hostName, 128, MPI_CHAR, &bogus, 128, MPI_CHAR, 0, managerIntercomm) != MPI_SUCCESS) {
     NL_fatal("err","Failed_MPI_Gather", "", "");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
   }
   NL_info("dbg", "stageleader.MPI_Gather.end", "", "");

   /* Get to work setting up the stageslices */

   /* How large is the set of potentially available nodes? */
   NL_info("dbg", "stageleader.getUniverseSize.start","","");
   if (MPI_Attr_get(MPI_COMM_WORLD, MPI_UNIVERSE_SIZE,
                &universe_sizep, &flag) != MPI_SUCCESS){
     NL_fatal("err","Failed_MPI_Attr_get", "", "");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
   }
   if (!flag) {
     NL_info("dbg", "This_MPI_does_not_support_UNIVERSE_SIZE.", "", "");
     scanf("%d", &universe_size);
   } else universe_size = *universe_sizep;
   NL_info("dbg", "stageleader.getUniverseSize.end",
           "UNIVERSE_SIZE=i", universe_size);

   NL_info("dbg", "stageleader.startStageSlices.start", "", "");
   /* if nodeList can be opened, read it in to form the list of nodes to use */
                                                                                
   FILE *f;
   ssize_t nChar;
   size_t len;
   char *name;
   int nNames = 0;
   int l;
   char nodeNames[MAX_NODE_COUNT][128];
                                                                                
   if ((f = fopen(nodeList, "r")) != (FILE *)NULL) {
     while (nNames < universe_size) {
       name = NULL;
       /* getline should really be replaced here - it's 
	  implicitly declared according to GCC - maybe 
	  GNU extensions are disabled here? */
       nChar = getline(&name, &len, f);
       if (nChar == -1) {
         /* err */
       }
       /* trim off newline */
       l = strlen(name) - 1;
       name[l] = '\0';
       strncpy(nodeNames[nNames], name, 128);
       free(name);
       nNames++;
     }
     printf("Stageleader: opened nodeList and got %i nodes\n", nNames);
     fclose(f);
   } else {
     /* don't use the nodeNames */
     printf("Stageleader: couldn't open nodeList at %s\n", nodeList);
   }



   /* Start stageslices for each (stage,CCD)  pair
    *  i.e. for each slice, spawn  "available-node" processes,
    *             where each node will process the appropriate count of 
    *             nearest neighbor CCDs.
    * Each spawning will create a COMM group for an image's full set of CCDs 
    * being processed by the same stage
    */
   /* Since this code itself is being parallel processed with one process 
    * per stage, a single mpispawn will actually initiate the CCD Slice 
    * processes on each stage.
    */

   // char *blocks_of_argv[MAX_NODE_COUNT * 8*sizeof(char *)];
   char **array_of_argv[MAX_NODE_COUNT];
   char *array_of_progs[MAX_NODE_COUNT];
   int array_of_maxprocs[MAX_NODE_COUNT];
   MPI_Info array_of_info[MAX_NODE_COUNT];
   int array_of_errcodes[MAX_NODE_COUNT];
                                                                                
   int k;
   char stageslice_program[MAX_PATH_SIZE];
   snprintf(stageslice_program, MAX_PATH_SIZE, "stageslice");
   sprintf(stageNumStr,"%i",stageNum);
                                                                                
   for (k = 0; k < universe_size; k++) {
     char **this_argv;
     this_argv = (char**) malloc(8 * sizeof(char**));
     array_of_argv[k] = this_argv;
     this_argv[0] = strdup(stageNumStr);
     this_argv[1] = strdup(numSlicesStr);
     this_argv[2] = WD;
     this_argv[3] = StageWorkerPath;
     this_argv[4] = policyDir;
     this_argv[5] = pipelinePolicy;
     this_argv[6] = diskPolicy;
     this_argv[7] = NULL;
     int numkeys;
                                                                                
     array_of_progs[k] = strdup(stageslice_program);
     array_of_maxprocs[k] =  1;
     if (nNames > 0) {
       MPI_Info_create(&array_of_info[k]);
       if (MPI_Info_set(array_of_info[k], "host", nodeNames[k]) != MPI_SUCCESS)
	 {
	   fprintf(stderr,"ERROR: Stageleader: MPI_info_set(): failed to set key to \"host\"\n");
	   NL_warn("err", "Failed:_MPI_Info_set","","");
	 }
       MPI_Info_get_nkeys(array_of_info[k], &numkeys);
       printf("stageleader: got %i info keys for proc %i\n", numkeys, k);
     } else {
       array_of_info[k] =  MPI_INFO_NULL;
     }
     array_of_errcodes[k] =  MPI_ERRCODES_IGNORE;
     /* the above causes a compiler warning, but there's no obvious
	way around it */
   }
   
   printf("stageleader: nNames = %i\n", nNames);


   if (MPI_Comm_spawn_multiple(universe_size, array_of_progs, array_of_argv,
               array_of_maxprocs, array_of_info, 0, MPI_COMM_SELF,
               &stageIntercomm, array_of_errcodes) != MPI_SUCCESS){
     NL_fatal("err", "Failed MPI_Comm_spawn8","", "");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
   }

   NL_info("dbg", "stageleader.startStageSlices.end", "", "");

   /*
    * Parallel code here. The communicator "stageIntercomm" can be used
    * to communicate with the spawned processes, which have ranks 0,..
    * MPI_UNIVERSE_SIZE-1 in the remote group of the intercommunicator
    * "stageIntercomm".
    */

   /* Gather the array of PID's and hostnames from stageslice processes */

   NL_info("dbg", "stageleader.MPI_Intercomm_merge.start", "", "");

   if (MPI_Intercomm_merge(stageIntercomm, 0, &stageBarrierComm) != MPI_SUCCESS){
     NL_fatal("err","Failed_MPI_Intercomm_merge", "", "");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
   }
   NL_info("dbg", "stageleader.MPI_Intercomm_merge.end", "", "");
   NL_info("dbg", "stageleader.MPI_Gather2.start", "", "");

   if (MPI_Gather(&bogus, 1, MPI_INT, pidList, 1, MPI_INT, MPI_ROOT, stageIntercomm) != MPI_SUCCESS) {
     NL_fatal("err","Failed_MPI_Gather", "", "");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
   }

   if (MPI_Gather(&bogus, 128, MPI_CHAR, hostList, 128, MPI_CHAR, MPI_ROOT, stageIntercomm) != MPI_SUCCESS ){
     NL_fatal("err","Failed_MPI_Gather", "","");
     NL_logger_del();
     MPI_Finalize();
     exit(1);
   }

   NL_info("dbg", "stageleader.MPI_Gather2.end", "", "");
   
   int kk;
   for (kk = 0 ; kk < universe_size; kk++) {
     NL_info("dbg","Successfully_created_SSlice_Process",
             "SSLICE_NUM=iSSLICE_PID=iSSLICE_HOST=s",
             kk, pidList[kk], hostList[kk]);
   }
   pidListLen = universe_size;

   /* activate signal handler */
   signal(SIGTERM, handleTERM);

   time_t t;
   char nextEntry[MAX_PATH_SIZE + 1];
   int nextEntryLen;

   nextEntry[MAX_PATH_SIZE]='\0';
   nextEntryLen = MAX_PATH_SIZE;

   /*
    * nextAction: 
    * this is the parameter to be sent/received to/from  
    * the other stageleaders with MPI_Ssend and MPI_Recv.

    * Previously, it was just bogus, but now it will have meaning; 
    * particularly it will be either KEEP_RUNNING or LETS_DIE_NOW,
    * and this stageleader should behave accordingly!

   */

   int nextAction = KEEP_RUNNING;    


   /* overall processing loop for this stage */
   /* 
      psuedocode:  

      repeat the following until terminate flag is set:

        wait for MPI_Recv.
	(stage 0 is always waiting for sigTERM.)
	if this is stage 0 and we've received sigTERM (killed == 1)
	  OR
        if this is stage >= 1 and we've received LETS_DIE_NOW as 
	   our message from MPI_Recv:
	   send LETS_DIE_NOW to the next stage.
	   set the terminate flag.
	otherwise:
	   if this is stage 0: 
	      look in the input queue for work.

	   wait for the stageslices to do work.

	   send KEEP_RUNNING to the next stageleader; blocking 
	     until received.

	 repeat.

   */

   int loop=0;
   int terminateFlag = 0;
   while (!terminateFlag) 
     {


     loop++;



     if ((stageNum > 0) || (loop > 1))
       {
	 /* wait for input from stageNum-1 */
	 /* MPI Recv(void* buf, int count, MPI Datatype datatype, int source, */
	 /* int tag, MPI Comm comm, MPI Status *status) */
	 int previousStageNum;
	 if (stageNum == 0) 
	   { 
	     previousStageNum = numStages - 1;
	   }
	 else 
	   { 
	     previousStageNum = stageNum - 1; 
	   }
	 NL_info("dbg", "stageleader.MPI_Recv.start", "WAITINGFOR=iITERATION=i", previousStageNum,loop);
	 if (MPI_Recv(&nextAction, 1, MPI_INTEGER, previousStageNum, STAGE_SYNC_TAG, stageLeaderComm, &status) != MPI_SUCCESS){
	   NL_fatal("err","Failed_MPI_Recv", "WAITINGFOR=i", previousStageNum);
	   NL_logger_del();
	   MPI_Finalize();
	   exit(1);
	 }
	 NL_info("dbg", "stageleader.MPI_Recv.end", "WAITINGFOR=iITERATION=iNEXTACTION=i", stageNum -1, loop,nextAction);
       }


     if ( 
	 ((stageNum == 0) && (killed))
	 ||
	 ((stageNum > 0) && (nextAction == LETS_DIE_NOW))
	 )
       {
	 nextAction = LETS_DIE_NOW;
	 terminateFlag = 1;
       }
     else /* not terminating this time around */
       {
	 if (stageNum==0) 
	   {
	     /* wait for input from queue */
#ifdef USE_IOQUEUE
	     /* use returned nextEntryLen to ensure string termination */
	     if ( get_oldster(inputQ,nextEntryLen,&nextEntry) != 0) {
	       NL_fatal("err","Failed_getting_next_inQ_item","","");
	       NL_logger_del();
	       MPI_Finalize();
	       exit(1);
	     }
	     printf("Got New INPUT from Q:%s item:%s\n", inputQ,nextEntry);
	     NL_info("dbg","next_inQ_item", "inQ=s", nextEntry);
#endif
	   }
	 
	 /* do the actual stageslice work */
	 /* entering the top barrier releases stageslices to do work */
	 t = time(0);
	 NL_info("dbg","stageleader.main.MPI_Barrier1.start", "ITERATION=i", loop);
	 if (MPI_Barrier(stageBarrierComm) != MPI_SUCCESS){
	   NL_fatal("err","Failed_MPI_Barrier", "", "");
	   NL_logger_del();
	   MPI_Finalize();
	   exit(1);
	 }
	 
	 NL_info("dbg","stageleader.main.MPI_Barrier1.end",
		 "ITERATION=i",loop);
	 
     
	 /* bottom barrier is entered after stageslices are done with their work */
	 NL_info("dbg","stageleader.main.MPI_Barrier2.start", "ITERATION=i", loop);
	 
	 if (MPI_Barrier(stageBarrierComm) != MPI_SUCCESS){
	   NL_fatal("err","Failed_MPI_Barrier", "ITERATON=i", loop);
	   NL_logger_del();
	   MPI_Finalize();
	   exit(1);
	 }
	 t = time(0);
	 NL_info("dbg","stageleader.main.MPI_Barrier2.end", "ITERATION=i", loop);
	 

#ifdef USE_IOQUEUE
	 /* remove the just-processed entry from the inputQ */
	 if (stageNum == 0){
	   NL_info("dbg","stageleader.main.unlink.start","FILE=s", nextEntry);
	   if (unlink(nextEntry) != 0){
	     NL_fatal("err","Failed_Unlink", "FILE=s", nextEntry);
	     NL_logger_del();
	     perror("Bad unlink");
	     MPI_Finalize();
	     exit(1);
	   }
	   NL_info("dbg","stageleader.main.unlink.end","FILE=s", nextEntry);
	 }
#endif
	 nextAction = KEEP_RUNNING;
       }

     /* send appropriate message to downstream stage - either  
      * "KEEP RUNNING" or "LETS DIE NOW", depending on which is set
     */
     
     int sendTarget;
     if (stageNum == numStages - 1) 
       { 
	 sendTarget = 0;
       }
     else 
       {
	 sendTarget = stageNum + 1;
       }
     NL_info("dbg", "stageleader.MPI_Ssend.start", "SENDTARGET=iITERATION=iNEXTACTION=i", sendTarget,loop,nextAction);
     if (MPI_Ssend(&nextAction, 1, MPI_INTEGER, sendTarget, STAGE_SYNC_TAG, stageLeaderComm) != MPI_SUCCESS){
       NL_fatal("err","Failed_MPI_Ssend", "", "");
       NL_logger_del();
       MPI_Finalize();
       exit(1);
     }
     NL_info("dbg", "stageleader.MPI_Ssend.end", "SENDTARGET=iITERATION=iNEXTACTION", sendTarget,loop,nextAction);
     /* return to 'while not terminateFlag' loop */
     }
   
   
   if (MPI_Finalize() != MPI_SUCCESS){
     NL_fatal("err","Failed MPI_Finalize", "", "");
     NL_logger_del();
     exit(1);
   }
   NL_info("dbg","stageleader.main.end", "", "");
   NL_logger_del();
   exit(0); 
}
