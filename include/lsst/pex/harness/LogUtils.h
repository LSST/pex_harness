// -*- lsst-c++ -*-
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
#include <unistd.h>
#include <vector>
#include <fstream>
#include <iostream>
#include <istream>
#include <ostream>
#include <sstream>

#include "lsst/pex/policy/Policy.h"
#include "lsst/utils/Utils.h"

#include "lsst/pex/harness/TracingLog.h"
#include "lsst/daf/base/PropertySet.h"
#include "lsst/pex/logging/Component.h"
#include "lsst/pex/logging/LogRecord.h"
#include "lsst/pex/logging/LogDestination.h"
#include "lsst/pex/logging/LogFormatter.h"
#include "lsst/pex/logging/Log.h"
#include "lsst/pex/logging/DualLog.h"
#include "lsst/pex/logging/ScreenLog.h"
#include "lsst/ctrl/events/EventLog.h"
#include "lsst/pex/exceptions.h"
#include <boost/shared_ptr.hpp>

using namespace lsst::daf::base;
using namespace lsst::pex::harness;

using namespace std;
using namespace lsst;
using namespace boost;

using lsst::pex::logging::Log;

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
                                const std::string& logdir
                                ); 

    void initializeSliceLogger(bool isLocalLogMode, //!< A flag for writing logs to local files
                                const std::string& name,
                                const std::string& runId,
                                const std::string& logdir,
                                const int rank
                            );


    TracingLog& getLogger() {
        return pipelineLog;
    }

    void setEventBrokerHost(const std::string& host) {
        _evbHost = host;
    }
    const std::string& getEventBrokerHost() {  return _evbHost;  }

    TracingLog pipelineLog;
    std::string _evbHost;
    ofstream* outlog;

};

} // namespace harness 

} // namespace pex 

} // namespace lsst

#endif // LSST_PEX_HARNESS_LOGUTILS_H 

