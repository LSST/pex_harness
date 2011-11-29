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
 
%define harness_DOCSTRING
"
Access to the C++ harness classes from the lsst.pex.harness module
"
%enddef

%feature("autodoc", "1");
%module(package="lsst.pex.harness", docstring=harness_DOCSTRING) harnessLib


%{
#include "lsst/daf/base.h"
#include "lsst/pex/exceptions.h"
#include "lsst/pex/logging.h"
#include "lsst/pex/harness/LogUtils.h"
%}

%include "lsst/p_lsstSwig.i"
%lsst_exceptions()

%import "lsst/pex/logging/loggingLib.i"

%include "lsst/pex/harness/LogUtils.h"

