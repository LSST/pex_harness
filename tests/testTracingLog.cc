/**
 * @file 
 * @brief tests the TracingLog class
 */
#include "lsst/pex/harness/TracingLog.h"

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE testTracingLog
#include <boost/test/unit_test.hpp>

using lsst::pex::harness::TracingLog;
using lsst::pex::logging::Log;

BOOST_AUTO_TEST_CASE( test_TracingLog )
{
    TracingLog rtr = TracingLog(Log::getDefaultLog(), "test");
    rtr.setThreshold(TracingLog::TRACE);
    TracingLog *tr = rtr.createForTraceBlock("api");
    tr->log(Log::INFO, "message");
    tr->done();
    delete tr;
}
