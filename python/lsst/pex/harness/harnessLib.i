%define harness_DOCSTRING
"
Access to the C++ harness classes from the lsst.pex.harness module
"
%enddef

%feature("autodoc", "1");
%module(package="lsst.pex.harness", docstring=harness_DOCSTRING,  "directors=1") harnessLib


%{
#include "lsst/utils/Utils.h"
#include "lsst/daf/base/Citizen.h"
#include "lsst/pex/exceptions.h"
#include "lsst/pex/policy/Policy.h"
#include "lsst/pex/policy/Dictionary.h"
#include "lsst/daf/base/PropertySet.h"
#include "lsst/daf/persistence/PropertySetFormatter.h"
#include "lsst/pex/logging/Log.h"
#include "lsst/pex/logging/ScreenLog.h"
#include "lsst/pex/logging/DualLog.h"
#include "lsst/pex/logging/LogRecord.h"
#include "lsst/pex/logging/Debug.h"
#include "lsst/pex/harness/TracingLog.h"
#include "lsst/pex/harness/LogUtils.h"
%}

%inline %{
namespace lsst { namespace pex { namespace harness { } } }
namespace lsst { namespace daf { namespace base { } } }
namespace lsst { namespace daf { namespace persistence { } } }
namespace lsst { namespace pex { namespace policy { } } }
namespace lsst { namespace pex { namespace logging { } } }
namespace lsst { namespace pex { namespace exceptions { } } }
namespace boost { namespace filesystem {} }

using namespace lsst;
using namespace lsst::pex::harness;
using namespace lsst::daf::base;
using namespace lsst::daf::persistence;
using namespace lsst::pex::policy;
using namespace lsst::pex::logging;
using namespace lsst::pex::exceptions;
%}

%init %{
%}

%pythoncode %{
import lsst.daf.base
import lsst.daf.persistence
import lsst.pex.policy
import lsst.pex.logging
import lsst.pex.harness
%}


%include "lsst/p_lsstSwig.i"
%lsst_exceptions()

%include "std_string.i"
%include "std_set.i"
%include "lsst/utils/Utils.h"

%import "lsst/daf/base/baseLib.i"
%import "lsst/pex/logging/loggingLib.i"
%import "lsst/pex/policy/policyLib.i"


%import "lsst/daf/base/Citizen.h"
%import "lsst/daf/base/PropertySet.h"
%import "lsst/daf/persistence/PropertySetFormatter.h"
%import "lsst/pex/exceptions.h"
%import "lsst/pex/logging/Debug.h"
%import "lsst/pex/logging/Log.h"
%import "lsst/pex/logging/LogRecord.h"
%import "lsst/pex/policy/Policy.h"

%include "lsst/pex/harness/TracingLog.h"
%include "lsst/pex/harness/LogUtils.h"

