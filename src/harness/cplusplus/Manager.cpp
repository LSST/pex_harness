#include "Manager.hpp"
/* make sure to se comments at top of Manager.hpp */

PipelineOptions PipelineOptionReader::readOptionsFromCommandLine(int argc, char** argv)
{
  int opt, longopt_index;
  PipelineOptions po;
  static struct option long_options[] = {
       {"policyDir", 1, NULL, 'D'},
       {"diskPolicy", 1, NULL, 'd'},
       {"pipelinePolicy", 1, NULL, 'p'},
       {"pipelineExe", 1, NULL, 'e'},
       {"numCCD", 1, NULL, 'c'},
       {"numStage", 1, NULL, 's'},
       {"inputQ", 1, NULL, 'q'},
       {"nodeList", 1, NULL, 'n'},
       {"help", 0, NULL, 'h'},
       {"usage", 0, NULL, 'h'},
       {NULL, 0, NULL, 0} /* marks end-of-list */
   };
   /* >>>>>>>>>>>>>>>>>>>>>>>>>>    USE MALLOC   >>>>>>>>>>>>>>>>>>>>>> */
   char policyDir[MAX_PATH_SIZE];
   char diskPolicy[MAX_PATH_SIZE];
   char pipelinePolicy[MAX_PATH_SIZE];
   char pipelineExe[MAX_PATH_SIZE];
   char numSlice[256];
   char numStageStr[256];
   char inputQ[MAX_PATH_SIZE];
   char nodeList[MAX_PATH_SIZE];
   int numStage;
   /* >>>>>>>>>>>>>>>>>>>>>>>>>>    USE MALLOC   >>>>>>>>>>>>>>>>>>>>>> */

   while ((opt = getopt_long(argc, argv, "D:d:p:e:c:s:n:h", long_options,
	 &longopt_index)) > 0) {
      switch (opt) {
         case 'D':
            strncpy(policyDir,optarg,MAX_PATH_SIZE);
            break;
         case 'd':
            strncpy(diskPolicy,optarg,MAX_PATH_SIZE);
            break;
         case 'p':
            strncpy(pipelinePolicy,optarg,MAX_PATH_SIZE);
            break;
         case 'e':
            strncpy(pipelineExe,optarg,MAX_PATH_SIZE);
            break;
         case 'c':
            strncpy(numSlice,optarg,256);
            break;
         case 's':
            strncpy(numStageStr,optarg,256);
            numStage = atoi(numStageStr);
            break;
         case 'q':
            strncpy(inputQ,optarg,MAX_PATH_SIZE);
            break;
         case 'n':
            strncpy(nodeList,optarg,MAX_PATH_SIZE);
            break;
         case '?':
         case 'h':
         default:
            /* getopt will also print an error message for you right here */
            fprintf(stderr,"Bad parameters passed to manager\n");
            fprintf(stderr, "usage: %s [options]\n", basename(argv[0]));
            fprintf(stderr, "  -D, --policyDir ARG      directory containing policies (required)\n");
            fprintf(stderr, "  -d, --diskPolicy ARG     disk locator policy (required)\n");
            fprintf(stderr, "  -p, --pipelinePolicy ARG      pipeline characteristics policy (required)\n");
            fprintf(stderr, "  -p, --pipelineExe ARG      full pathname to pipeline executable (required)\n");
            fprintf(stderr, "  -s, --numCCD ARG     number of CCDs per mosaic (required)\n");
            fprintf(stderr, "  -s, --numStage [ARG]  number of stages in pipeline (required)\n");
            fprintf(stderr, "  -q, --inputQ [ARG]  input queue for stage0 (required)\n");
            fprintf(stderr, "  -n, --nodeList [ARG]  file containing list of nodes for stageslices (optional)\n");
            fflush(stderr);
	    NL_fatal("err","Manager_main:_bad_params", "","");
	    throw LSSTException(std::string("ERROR: pipeline executable is empty string!"));
      }
   }

   po.policyDir = policyDir;
   po.diskPolicy = diskPolicy;
   po.pipelinePolicy = pipelinePolicy;
   po.pipelineExe = pipelineExe;
   po.inputQ  = inputQ;
   po.numStageStr = numStageStr;
   po.nodesList = nodeList;
   po.numSlice = atoi(numSlice);
   po.numStage = numStage;
   return po;
}







/* * * * * * * * * * PIPELINE MANAGER * * * * * * * * * */


string PipelineManager::getStageExec(int stageNum, string WD) 
{ 
  /* getStageExec	get pathname, from the Pipeline policy, of code to be 
   *			executed for requested Stage by the Ppeline harness.
   *	input parameters
   *		stageNum	number of the stage
   *		WD		path to directory receiving the temp file
   *	output parameter
   *		none
   *	return
   *		pathname of the pipeline code to be executed by harness.
   *		Returned memory MUST BE FREED by the calling routine.
   */


  int mypid;
  mypid = getpid();
  
  char getParam[MAX_PATH_SIZE];
  sprintf(getParam,"%s/GetParam.%d.%d",WD.c_str(),stageNum,mypid);
  
  char process_cmd[512];
  sprintf(process_cmd, "PolicyExtract.csh %d %d > %s", stageNum, 5, getParam);
  
  int ret;
  ret = system(process_cmd);
  if ( ret != 0) {
    NL_fatal("err","Failed_getStageExec_exec", "RET=d", ret);
    throw LSSTException(std::string("Error: getStageExec failed execution of command"));
  }
  
  FILE *paramFID;
  paramFID = fopen(getParam,"r");
  char *param;
  string paramAsString;
  ret = fscanf(paramFID,"%as",&param);
  if ( ret < 1) {
    NL_fatal("err","Failed_getStageExec_fetch", "RET=d", ret);
    throw LSSTException(std::string("Error: getStageExec failed fscanf."));
  }
  fclose(paramFID);
  unlink(getParam);
  paramAsString = param;
  return paramAsString;
}














void PipelineManager::retrieveMPIInfo()
{
  int* universe_sizep;

  NL_info("dbg", "manager.main.retrieveMPIInfo.start", "", "");

  world_size = MPI::COMM_WORLD.Get_size();
  
  if (world_size != 1) {
    NL_fatal("err", "Management_too_heavy_(world_size!=1)", "", "");
    throw LSSTException(std::string("Error: PipelineManager::retrieveMPIInfo: Got world_size != 1"));
  }
  
  world_rank = MPI::COMM_WORLD.Get_rank();
  
  MPI::COMM_WORLD.Get_attr(MPI::UNIVERSE_SIZE, &universe_sizep);
  universe_sizep = &universe_size;
  
  if (universe_size == 1) {
    NL_fatal("err", "No_room_to_start_stageleaders","", "");
    throw LSSTException(std::string("Error: PipelineManager::retrieveMPIInfo: No room for Stageleaders"));
  }
  
  NL_info("dbg", "manager.main.retrieveMPIInfo.end", 
	  "PIPELINE_MANAGER_RANK=iWORLD_SIZE=iUNIVERSE_SIZE=i", 
	  world_rank, world_size,universe_size);
  
  
}




void PipelineManager::spawnStageLeaders()
{

  /* Set up args for the individual stageleaders.   Each takes: 
   * Input args
   * jmyers - stageNum is no longer a parameter!
   * argv[0] - NumSlices       - the number of slices 
   * argv[1] - WorkDir         - full path name to working directory that 
   *				  will be used for log files
   * argv[2] - pipelineExe     - full path name to pipeline executable 
   * argv[3] - policyDir       - full path name to policy root dir
   * argv[4] - pipelinePolicy  - name of pipeline policy file 
   * argv[5] - diskPolicy      - name of disk locator policy file 
   * argv[6] - inputQ          - name of stage0 input queue
   * argv[7] - nodeList        - full path name of file containing nodes 
   *				  to use for stageslices; ignored if NULL
   */
  
  char **array_of_argv[MAX_STAGES];
  char *array_of_progs[MAX_STAGES];
  int array_of_maxprocs[MAX_STAGES];
  MPI::Info array_of_info[MAX_STAGES];

  string stageleader_program = "stageleader"; 
  
  int k;
  string plExe;

  NL_info("dbg", "manager.main.MPI_Comm_spawn_multiple.start", 
	  "NUMBER=iCODE=s",numStage,stageleader_program.c_str());

  for (k = 0; k < numStage; k++) {
    char **this_argv;
    this_argv = (char**)malloc(9*sizeof(char *));
     
    if ( (plExe = getStageExec(k,PWD) ) == "") {
      NL_info("err","manager.main.getStageExec",
	      "STAGE_NUM=ipolicyDir=sPWD=s",k, policyDir.c_str(),PWD.c_str());
      throw LSSTException(std::string("ERROR: pipeline executable is empty string!"));
    }
    
    array_of_argv[k] = this_argv;
    char * numSliceStr = (char*)malloc(sizeof(char*)*100); // 100 is *way* more than enough!
    sprintf(numSliceStr,"%d", numSlice);
    this_argv[0] = strdup(numSliceStr);
    this_argv[1] = strdup(PWD.c_str());
    this_argv[2] = strdup(plExe.c_str());
    this_argv[3] = strdup(policyDir.c_str());
    this_argv[4] = strdup(pipelinePolicy.c_str());
    this_argv[5] = strdup(diskPolicy.c_str());
    this_argv[6] = strdup(inputQ.c_str());
    this_argv[7] = strdup(nodesList.c_str());
    this_argv[8] = NULL;
    
    array_of_progs[k] = strdup(stageleader_program.c_str());
    array_of_maxprocs[k] =  1;
    array_of_info[k] =  MPI::INFO_NULL;
    NL_info("dbg", "manager.main.Starting_Stageleader", 
	    "STAGELEADER_NUM=iTOTALLEADERS=iARG1=sARG2=s,ARG3=s",	     
	    k, numStage, this_argv[1], this_argv[2], this_argv[3]);
  }

  everyone = MPI::COMM_WORLD.Spawn_multiple(numStage, (const char**) array_of_progs, (const char***)array_of_argv,
					    array_of_maxprocs, array_of_info, MPI::ROOT); 
  // LIKELY BUG:  
  // last argument should be == 0 == MPI::ROOT, but MPI::ROOT has been misbehaving. in practice.cpp, it's -3 (?!)
  // if something goes wrong, look here first.

  NL_info("dbg", "manager.main.MPI_Comm_spawn_multiple.end", 
	  "", "");


}





void PipelineManager::collectStageLeaderPIDs()
{
  stageleaderPidList = (int*)malloc(sizeof(int)*numStage);
  stageleaderHostList = (string*)malloc(sizeof(int)*numStage);
  stageleaderPidListLen = numStage;
  NL_info("dbg", "manager.main.MPI_Gather.start", "MPI_ROOT=i", MPI::ROOT);
  
  int bogus;
  everyone.Gather(&bogus, 1, MPI::INT, stageleaderPidList, 1, MPI::INT, MPI::ROOT);
  // TBD: Last parameter is supposed to be MPI::ROOT... but MPI::ROOT has a way of erroring in practice.cpp.
  everyone.Gather(&bogus, 128, MPI::CHAR, stageleaderHostList, 128, MPI::CHAR, MPI::ROOT);
   
   NL_info("dbg", "manager.main.MPI_Gather.end", "", "");
   
   for (int k = 0 ; k < numStage; k++) {
     NL_info("dbg", "manager.main.stageleader_started", 
	     "STAGELEADER_NUM=iSTAGELEADERPID=iSTAGELEADERHOST=s",
	     k,stageleaderPidList[k],stageleaderHostList[k].c_str());
   }
   stageleaderPidListLen = numStage;
   
}








void PipelineManager::terminatePipeline()
{
  int k;
  char cmd[256];
  for (k = 0; k < stageleaderPidListLen; k++) {
    sprintf(cmd,"ssh %s kill -TERM %d", stageleaderHostList[k].c_str(), stageleaderPidList[k]);
    system(cmd);
    NL_info("dbg", "Killing_all_child_processes", "", ""); 
  }
  NL_fatal("err", "Manager_received_sigTerm.", "", "");
  MPI::Finalize();
  exit(0);
}



/*********** CONSTRUCTOR *****************/
PipelineManager::PipelineManager(PipelineOptions myOpts)
{
  PWD =  getenv("PWD");
  
  stageleaderPidListLen = 0;
  stageleaderPidList = NULL;
  stageleaderHostList = NULL;

  policyDir = myOpts.policyDir;
  diskPolicy = myOpts.diskPolicy;
  pipelinePolicy = myOpts.pipelinePolicy;
  pipelineExe = myOpts.pipelineExe;
  inputQ = myOpts.inputQ;
  numStageStr = myOpts.numStageStr;
  nodesList = myOpts.nodesList;
  numSlice = myOpts.numSlice;
  numStage = myOpts.numStage;
  retrieveMPIInfo();
}



/******* DESTRUCTOR **********/
PipelineManager::~PipelineManager()
{
  if (stageleaderPidList != NULL)
    free(stageleaderPidList);
  if (stageleaderHostList != NULL)
    free(stageleaderHostList);
}



void PipelineManager::runPipeline()
{
  if (NOTHIN_BUT_DEBUG)
    {
      cout << "PipelineManager::runPipeline Received Options: \n" << "\n" <<
	"policyDir = " << policyDir << "\n" <<
	"diskPolicy = " << diskPolicy << "\n" <<
	"pipelinePolicy = " << pipelinePolicy << "\n" <<
	"pipeline Exec = " << pipelineExe << "\n" <<
	"inputQ = " << inputQ << "\n" <<
	"numStageStr = " << numStageStr << "\n" <<
	"nodesList = " << nodesList << "\n" <<
	"numSlice = " << numSlice << "\n" <<
	"numStage = " << numStage << "\n";
      return;
    }

  spawnStageLeaders();

  collectStageLeaderPIDs();

}







/* * * * * * * *  * * * * * * * * * * * * * * * * * * * * * * * */
/* * * * * * * PER-PROCESS CODE (main and helpers)* * * * * * * */

PipelineManager* theManager;

void handleTERM(int signal)
{
  theManager->terminatePipeline();
}





int main(int argc, char** argv)
{
  // set up MPI_Init
  int dummy_argc;
  char ** dummy_argv;

  MPI::Init(dummy_argc, dummy_argv);

  MPI::COMM_WORLD.Set_errhandler(MPI::ERRORS_THROW_EXCEPTIONS);
  try // check for internal LSSTExceptions
    {
      try // check for MPI::Exceptions
	{
	  // set up loggers 
	  int pid = getpid();
	  string username;
	  username = getenv("USER");
	  
	  if (username == "")
	    {
	      fprintf(stderr, "manager.c: Got $USER == NULL!\n");
	    }
	  char* logfile = (char*)malloc(sizeof(char)*MAX_PATH_SIZE);
	  sprintf(logfile, "/tmp/lsst.harness.manager.%i.%s.log", pid, username.c_str());
	  NL_logger_module_const("dbg", logfile,  NL_LVL_INFO,
				 "MYPID:iPROG:sHOST:s", pid, "manager", ipaddr());
      
	  /* this module, "err", logs by default a constant field-value pair:
	   * SOURCE=manager.c 
	   * by default, it will record the hostname.
	   */

	  NL_logger_module_const("err", logfile,   NL_LVL_ERROR, 
				 "MYPID:iPROG:sHOST:s", pid, "manager", ipaddr());
      
	  // cheat a little and log the start and end of main
	  NL_info("dbg", "manager.main.start", "", "" );
      
	  //read command-line options
	  PipelineOptions myRunOptions;
	  PipelineOptionReader myPOR;
	  myRunOptions = myPOR.readOptionsFromCommandLine(argc, argv);
	  // run the manager
	  PipelineManager myPipelineManager(myRunOptions);
	  theManager = &myPipelineManager; // for signalHandler
	  myPipelineManager.runPipeline();

	  //set up a signal handler for sigTERM - it will call the TerminatePipeline
	  // function of PipelineManager (slight hack, though)
	  // wait indefinitely
	  signal(SIGTERM, handleTERM);
	  pause();

	  // wrap up our MPI calls
	  MPI::Finalize();
	  // cheat a little and log the start and end of main
	  NL_info("dbg", "manager.main.end", "", "");
	  NL_logger_del();
  
	  return 0;
	}
      catch (MPI::Exception e)
	{
	  NL_fatal("err", "Manager.main:_Unexpected_MPI_error", "ERRCODE=iERRCLASS=iERRSTRING=s",
		   e.Get_error_code(),
		   e.Get_error_class(),
		   e.Get_error_string());
	  MPI::Finalize();
	  NL_logger_del();
	  return -1;
	}
    }
  catch (LSSTException f)
    {
      NL_fatal("err", "Manager.main:_Unexpected_internal_error", "ERRSTRING=s",
	       f.getErrorString());
      MPI::Finalize();
      NL_logger_del();
      return -2;
    }
}
