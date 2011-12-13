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
 
/** \file LogUtils.h
  *
  * \ingroup harness
  *
  * \brief   LogUtils class creates and manages the logger of a Pipeline and Slice.
  *
  * \author  Greg Daues, NCSA
  */

#ifndef LSST_PEX_HARNESS_LOGUTILS_H
#define LSST_PEX_HARNESS_LOGUTILS_H


#include <string>
#include <ostream>
#include <fstream>

#include "lsst/pex/logging/BlockTimingLog.h"


namespace lsst {
namespace pex {
namespace harness {


/**
  * \brief   Pipeline class manages the operation of a multi-stage parallel pipeline.
  *
  *          Pipeline spawns Slice workers and coordinates serial and parallel processing 
  *          between the main thread and the workers by means of MPI communciations.
  *          Pipeline loops over the collection of Stages for processing on Image.
  *          The Pipeline is configured by reading a Policy file.
  */

class LogUtils {
public:
    LogUtils(); // constructor

    ~LogUtils(); // destructor

    void initializeLogger(bool isLocalLogMode,  //!< A flag for writing logs to local files
                          const std::string& name,
                          const std::string& runId,
                          const std::string& logdir,
                          const std::string& workerid,
                          int resourceUsageFlags = 0  
                          ); 

    void initializeSliceLogger(bool isLocalLogMode, //!< A flag for writing logs to local files
                               const std::string& name,
                               const std::string& runId,
                               const std::string& logdir,
                               const int rank,
                               const std::string& workerid,
                               int resourceUsageFlags = 0 
                               );


    lsst::pex::logging::BlockTimingLog& getLogger() {
        return pipelineLog;
    }

    void setEventBrokerHost(const std::string& host) {
        _evbHost = host;
    }
    const std::string& getEventBrokerHost() {  return _evbHost;  }

    lsst::pex::logging::BlockTimingLog pipelineLog;
    std::string _evbHost;
    std::ofstream* outlog;

};

/**
 * @brief create and configure logging for a harness application 
 * 
 * This will create and configure a default log needed to send all log
 * messages through the event system.  It then creates a child log to 
 * be used by a harness class instance (i.e. Pipeline or Slice).
 * @param runId             the production run identifier that harness is 
 *                             running under
 * @param sliceId           the slice identifier (-1 for the master process)
 * @param eventBrokerHost   the hostname where the desired event broker is 
 *                             running
 * @param pipename          the name used to identify the pipeline
 * @param messageStrm       if non-null, a (file) stream to replicate the 
 *                             messages in.
 * @param logname           the name to give to the harness logger (default:
 *                             "harness")
 * @param resourceUsageFlags  the OR-ed flags that control the resource usage
 *                             data to collect; see BlockTimingLog. (default:
 *                             NOUDATA, no resource usage data.)
 */
lsst::pex::logging::BlockTimingLog *setupHarnessLogging(
    const std::string& runId, int sliceId, 
    const std::string& eventBrokerHost="", 
    const std::string& pipename="unnamed",
    const std::string& workerid="-1",
    std::ostream *messageStrm=0,
    const std::string& logname="harness",
    int resourceUsageFlags=0);

} // namespace harness 

} // namespace pex 

} // namespace lsst

#endif // LSST_PEX_HARNESS_LOGUTILS_H 

