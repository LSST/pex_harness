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
