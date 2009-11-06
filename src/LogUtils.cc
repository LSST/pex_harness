// -*- lsst-c++ -*-
/** \file LogUtils.cc
  *
  * \ingroup harness
  *
  * \brief   LogUtils class is a utilities class for creation of the Pipeline and Slice logging.
  *
  * \author  Greg Daues, NCSA
  */
#include <cstring>
#include <sstream>

#include "lsst/pex/harness/LogUtils.h"

using lsst::pex::logging::Log;

namespace lsst {
namespace pex {
namespace harness {

/** 
 * Constructor.
 */
LogUtils::LogUtils() 
    : pipelineLog(Log::getDefaultLog(),"harness"), _evbHost(""), outlog(0) 
{ }

/** Destructor.
 */
LogUtils::~LogUtils(void) {
    delete outlog;
}

/** Initialize the logger "pipelineLog" to be used globally in the Pipeline class.
 *  Add an ofstream  Destination to the default logger if the localLogMode is True
 * local file is on
 */
void LogUtils::initializeLogger(bool isLocalLogMode,  //!< A flag for writing logs to local files
                                const std::string& name,
                                const std::string& runId  
                                ) {
    char *logfile = "Pipeline.log";
    if(isLocalLogMode) {
        /* Make output file stream   */
        outlog =  new ofstream(logfile);
    }
    boost::shared_ptr<TracingLog> 
        lp(setupHarnessLogging(std::string(runId), -1, _evbHost, name,
                               outlog, "harness.pipeline"));
                               
    pipelineLog = *lp;

    pipelineLog.format(Log::INFO, 
                       "Pipeline Logger initialized for pid=%d", getpid());
    if (outlog) 
        pipelineLog.format(Log::INFO, "replicating messages to %s", logfile);
}

/** Initialize the logger "sliceLog" to be used globally in the Slice class. 
 *  Add an ofstreamDestination to the default logger if the localLogMode is True
 */
void LogUtils::initializeSliceLogger(bool isLocalLogMode, //!< A flag for writing logs to local files
                                const std::string& name,
                                const std::string& runId,
                                const int rank
                            ) {

    std::string logfile;
    if(isLocalLogMode) {
        /* Make a log file name coded to the rank    */
        std::stringstream logfileBuffer;

        logfileBuffer << "Slice";
        logfileBuffer << rank;
        logfileBuffer << ".log";

        logfileBuffer >> logfile;

        /* Make output file stream   */
        outlog =  new ofstream(logfile.c_str());
    }

    boost::shared_ptr<TracingLog>
        lp(setupHarnessLogging(std::string(runId), rank, _evbHost,
                               name, outlog, "harness.slice"));
    pipelineLog = *lp;

    pipelineLog.format(Log::INFO, "Slice Logger initialized for pid=%d", getpid());

    if (outlog)
        pipelineLog.format(Log::INFO,
                        "replicating messages to %s", logfile.c_str());
}


}
}
}
