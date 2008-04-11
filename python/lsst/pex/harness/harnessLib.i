%define harness_DOCSTRING
"
Access to the C++ harness classes from the lsst.pex.harness module
"
%enddef

%feature("autodoc", "1");
%module(package="lsst.pex.harness", docstring=harness_DOCSTRING) harnessLib

%{
#include "lsst/pex/harness/Clipboard.h"
#include "lsst/pex/harness/Queue.h"
#include "lsst/pex/harness/Stage.h"
#include "lsst/pex/harness/Pipeline.h"
#include "lsst/pex/harness/Slice.h"
%}

%inline {
namespace lsst { namespace pex { namespace harness { } } }
}

%include "lsst/pex/harness/Clipboard.h"
%include "lsst/pex/harness/Queue.h"
%include "lsst/pex/harness/Stage.h"
%include "lsst/pex/harness/Pipeline.h"
%include "lsst/pex/harness/Slice.h"

