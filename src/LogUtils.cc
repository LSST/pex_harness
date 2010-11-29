// -*- lsst-c++ -*-

/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
 * 
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the LSST License Statement and 
 * the GNU General Public License along with this program.  If not, 
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */
 
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

#include "lsst/ctrl/events.h"
#include "lsst/ctrl/events/EventSystem.h"
#include "lsst/ctrl/events/EventLog.h"

using namespace std;

using lsst::ctrl::events::EventSystem;
using lsst::ctrl::events::EventLog;
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
                                const std::string& runId,
                                const std::string& logdir,
                                const std::string& workerid,
                                int resourceUsageFlags
                                ) {
    std::stringstream logfileBuffer;
    std::string logfile;

    if (logdir.length() > 0) { 
        logfileBuffer << logdir;
        logfileBuffer << "/";
    }

    logfileBuffer << "Pipeline.log";
    logfileBuffer >> logfile;

    if(isLocalLogMode) {
        /* Make output file stream   */
        outlog =  new ofstream(logfile.c_str());
    }

    /* Create LSSTLogging transmitter here. (Moved from setupHarnessLogging in TracingLog.cc 
       Need to re-check this for MPI SLices. */ 
    if (_evbHost.length() > 0) { 
        EventSystem& eventSystem = EventSystem::getDefaultEventSystem();
        /* eventSystem.createTransmitter(_evbHost, EventLog::LOGGING_TOPIC); */
        eventSystem.createTransmitter(_evbHost, "logging");
    }

    boost::shared_ptr<BlockTimingLog> 
        lp(setupHarnessLogging(std::string(runId), -1, _evbHost, name, workerid,
                               outlog, "harness.pipeline",resourceUsageFlags));
                               
                               
    pipelineLog = *lp;

    pipelineLog.format(Log::INFO, 
                       "Pipeline Logger initialized for pid=%d", getpid());
    if (outlog) 
        pipelineLog.format(Log::INFO, "replicating messages to %s", logfile.c_str());
}

/** Initialize the logger "sliceLog" to be used globally in the Slice class. 
 *  Add an ofstreamDestination to the default logger if the localLogMode is True
 */
void LogUtils::initializeSliceLogger(bool isLocalLogMode, //!< A flag for writing logs to local files
                                     const std::string& name,
                                     const std::string& runId,
                                     const std::string& logdir,
                                     const int rank,
                                     const std::string& workerid,
                                     int resourceUsageFlags
                                     ) 
{
    std::string logfile;
    if(isLocalLogMode) {
        /* Make a log file name coded to the rank    */
        std::stringstream logfileBuffer;

        if (logdir.length() > 0) {
            logfileBuffer << logdir;
            logfileBuffer << "/";
        }

        logfileBuffer << "Slice";
        logfileBuffer << rank;
        logfileBuffer << ".log";

        logfileBuffer >> logfile;

        /* Make output file stream   */
        outlog =  new ofstream(logfile.c_str());
    }

    
    boost::shared_ptr<BlockTimingLog>
        lp(setupHarnessLogging(std::string(runId), rank, _evbHost,
                               name, workerid, outlog, "harness.slice",
                               resourceUsageFlags));
    pipelineLog = *lp;

    pipelineLog.format(Log::INFO, "Slice Logger initialized for pid=%d", getpid());

    if (outlog)
        pipelineLog.format(Log::INFO,
                        "replicating messages to %s", logfile.c_str());
}

using lsst::ctrl::events::EventSystem;
using lsst::ctrl::events::EventLog;
using lsst::pex::logging::Log;
using lsst::pex::logging::Rec;
using lsst::pex::logging::LogFormatter;
using lsst::pex::logging::IndentedFormatter;
using lsst::pex::logging::LogDestination;

BlockTimingLog *setupHarnessLogging(const std::string& runId, int sliceId, 
                                    const std::string& eventBrokerHost, 
                                    const std::string& pipename,
                                    const std::string& workerid,
                                    std::ostream *messageStrm,
                                    const std::string& logname,
                                    int resourceUsageFlags)
{
    if (eventBrokerHost.length() > 0) {
        /*  Move this to LogUtils.cc and only call for the Pipeline 
            Re-examine this for MPI Slices 
        EventSystem& eventSystem = EventSystem::getDefaultEventSystem();
        eventSystem.createTransmitter(eventBrokerHost, "LSSTLogging");
        */ 
        EventLog::createDefaultLog(runId, sliceId);
    }
    Log& root = Log::getDefaultLog();
    root.addPreambleProperty("PIPELINE", pipename);
    root.addPreambleProperty("workerid", workerid);

    if (messageStrm != 0) {
        boost::shared_ptr<LogFormatter> frmtr(new IndentedFormatter(true));
        boost::shared_ptr<LogDestination> 
            dest(new LogDestination(messageStrm, frmtr, Log::DEBUG));
        root.addDestination(dest);
    }

    BlockTimingLog *out = 0;
    try {
        out = new BlockTimingLog(root, logname);
        out->setUsageFlags(resourceUsageFlags);
        out->format(Log::INFO, 
                    "Harness Logger initialized with message threshold = %i", 
                    out->getThreshold());
        return out;
    }
    catch (...) {
        delete out;
        throw;
    }
}

}
}
}
