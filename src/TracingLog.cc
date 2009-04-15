/**
 * @file TracingLog.cc
 * @author Ray Plante
 */

#include "lsst/pex/harness/TracingLog.h"
#include "lsst/ctrl/events.h"
#include "lsst/ctrl/events/EventSystem.h"
#include "lsst/ctrl/events/EventLog.h"

#include <fstream>

namespace lsst {
namespace pex {
namespace harness {

const int TracingLog::TRACE = logging::Log::INFO - 1;

const std::string TracingLog::STATUS("STATUS");
const std::string TracingLog::START("start");
const std::string TracingLog::END("end");

TracingLog::TracingLog(const logging::Log& parent, const std::string& name, 
                       int tracelev, const std::string& funcName) 
    : Log(parent, name), _tracelev(tracelev), _funcName(funcName)
{
    if (_funcName.length() == 0) _funcName = name;
    if (_tracelev == logging::Log::INHERIT_THRESHOLD) {
        const TracingLog *p = dynamic_cast<const TracingLog*>(&parent);
        if (p) {
            _tracelev = p->getTraceLevel();
        }
        else {
            _tracelev = TRACE;
        }
    }
}

TracingLog::~TracingLog() { }

using lsst::ctrl::events::EventSystem;
using lsst::ctrl::events::EventLog;
using lsst::pex::logging::Log;
using lsst::pex::logging::Rec;
using lsst::pex::logging::LogFormatter;
using lsst::pex::logging::IndentedFormatter;
using lsst::pex::logging::LogDestination;

TracingLog *setupHarnessLogging(const std::string& runId, int sliceId, 
                                const std::string& eventBrokerHost, 
                                const std::string& pipename,
                                std::ostream *messageStrm,
                                const std::string& logname)
{
    if (eventBrokerHost.length() > 0) {
        EventSystem& eventSystem = EventSystem::getDefaultEventSystem();
        eventSystem.createTransmitter(eventBrokerHost, "LSSTLogging");
        EventLog::createDefaultLog(runId, sliceId);
    }
    Log& root = Log::getDefaultLog();
    root.addPreambleProperty("pipeline", pipename);

    if (messageStrm != 0) {
        boost::shared_ptr<LogFormatter> frmtr(new IndentedFormatter(true));
        boost::shared_ptr<LogDestination> 
            dest(new LogDestination(messageStrm, frmtr, Log::DEBUG));
        root.addDestination(dest);
    }

    TracingLog *out = 0;
    try {
        out = new TracingLog(root, logname);
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

}}} // end lsst::pex::harness
