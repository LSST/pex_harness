// -*- lsst-c++ -*-
/**
 * @file TracingLog.h
 * @brief definition of the TracingLog class
 * @author Ray Plante
 */
#ifndef LSST_PEX_HARNESS_TRACINGLOG_H
#define LSST_PEX_HARNESS_TRACINGLOG_H

#include "lsst/pex/logging/LogRecord.h"
#include "lsst/pex/logging/Log.h"

namespace lsst {
namespace pex {
namespace harness {

namespace logging = lsst::pex::logging;

/**
 * a specialized Log that assists the harness in tracing execution flow.
 *
 * The motivation for this class is to provide uniformity in the log 
 * messages that indicate the start and finish of some section of code.  
 * This makes it easier to locate these records after execution and
 * calculate the time spent in the traced code.  
 */
class TracingLog : public logging::Log {
public:

    /**
     * default message level for messages that trace execution flow
     */
    static const int TRACE;  // = logging::Log::INFO - 1

    /**
     * the property name that will indicate what a trace message is marking
     */
    static const std::string STATUS;

    /**
     * the property to use to mark the start of a block of code being 
     * traced
     */
    static const std::string START;

    /**
     * the property to use to mark the start of a block of code being 
     * traced
     */
    static const std::string END;

    /**
     * construct a TracingLog.  
     * 
     * @param parent     the parent log to inherit attributes from 
     * @param name       the name to give the log relative to its parent
     * @param tracelev   the default level to use for the tracing messages
     * @param funcName   the name of the block of code (e.g. function) being 
     *                      traced.  If empty, the value given by name will 
     *                      be used instead.  This value will be used in the 
     *                      log message only.
     */
    TracingLog(const logging::Log& parent, const std::string& name, 
               int tracelev=TracingLog::TRACE, const std::string& funcName="");

    /**
     * create a copy of a TracingLog
     */
    TracingLog(const TracingLog& that) 
        : Log(*this), _tracelev(that._tracelev), _funcName(that._funcName)
    { }

    /**
     * reset this instance to another
     */
    TracingLog& operator=(const TracingLog& that) {
        Log::operator=(that);
        _tracelev = that._tracelev;
        _funcName = that._funcName;
        return *this;
    }

    /**
     * create and return a new child that should be used while tracing a 
     * function.  A "start" message will be logged to the new log as part
     * of the call.  Like createChildLog(), the caller is responsible for
     * deleting this new instance when finished with it.
     * @param name       the name to give the log relative to its parent
     * @param tracelev   the default level to use for the tracing messages.
     *                      If equal to Log::INHERIT_THRESHOLD (default),
     *                      it will be set to this log's tracing level.
     * @param funcName   the name of the block of code (e.g. function) being 
     *                      traced.  If empty, the value given by name will 
     *                      be used instead.  This value will be used in the 
     *                      log message only.
     */
    TracingLog *createForTraceBlock(const std::string& name, 
                                 int tracelev=logging::Log::INHERIT_THRESHOLD,
                                    const std::string& funcName="") 
    {
        TracingLog *out = new TracingLog(*this, name, tracelev, funcName);
        // out->setShowAll(true);
        out->start();
        return out;
    }

    /**
     * a synonym for createForTraceBlock
     */
    TracingLog *traceBlock(const std::string& name, 
                           int tracelev=logging::Log::INHERIT_THRESHOLD,
                           const std::string& funcName="") 
    {
        return createForTraceBlock(name, tracelev, funcName);
    }

    /**
     * delete the log
     */
    virtual ~TracingLog();

    /**
     * Indicate that the section of code being traced (e.g. a function body)
     * is starting.
     */
    void start() {
        if (sends(_tracelev)) {
            std::string msg("Starting ");
            msg += _funcName;
            log(_tracelev, msg, STATUS, START);
        }
    }

    /**
     * Indicate that the section of code being traced (e.g. a function body)
     * is starting.
     */
    void start(const std::string& funcName) {
        if (funcName.length() > 0) _funcName = funcName;
        start();
    }

    /**
     * Indicate that the section of code being traced (e.g. a function body)
     * is finished.
     */
    void done() {
        if (sends(_tracelev)) {
            std::string msg("Ending ");
            msg += _funcName;
            log(_tracelev, msg, STATUS, END);
        }
    }

    /**
     * return the tracing message level, the importance to be given to the 
     * start and end messages.
     */
    int getTraceLevel() const { return _tracelev; }

    /**
     * return the name of the code block being traced which will appear
     * in the start and end messages.  
     */
    const std::string& getFunctionName() const { return _funcName; }

private:
    int _tracelev;
    std::string _funcName;
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
 */
TracingLog *setupHarnessLogging(const std::string& runId, int sliceId, 
                                const std::string& eventBrokerHost="", 
                                const std::string& pipename="unnamed",
                                std::ostream *messageStrm=0,
                                const std::string& logname="harness");

}}}     // end lsst::pex::harness

#endif  // end LSST_PEX_LOG_H
