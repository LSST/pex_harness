# -*- python -*-
#
# Setup our environment
#
import glob, os.path, re, os
import lsst.SConsUtils as scons

dependencies = "boost mpich2 utils pex_policy pex_exceptions daf_base pex_logging daf_persistence ctrl_events python".split()

env = scons.makeEnv("pex_harness",
                    r"$HeadURL$",
                    [["boost", "boost/version.hpp", "boost_filesystem:C++"],
                     ["boost", "boost/regex.hpp", "boost_regex:C++"],
                     ["boost", "boost/serialization/serialization.hpp", "boost_serialization:C++"],
                     ["boost", "boost/serialization/base_object.hpp", "boost_serialization:C++"],
                     ["mpich2", "mpi.h", "mpich:C++"],
                     ["boost", "boost/mpi.hpp", "boost_mpi:C++"],
                     ["utils", "lsst/utils/Utils.h", "utils:C++"],
                     ["pex_exceptions", "lsst/pex/exceptions.h","pex_exceptions:C++"],
                     ["daf_base", "lsst/daf/base/Citizen.h", "pex_exceptions daf_base:C++"],
                     ["pex_logging", "lsst/pex/logging/Component.h", "pex_logging:C++"],
                     ["pex_policy", "lsst/pex/policy/Policy.h","pex_policy:C++"],
                     ["daf_persistence", "lsst/daf/persistence.h", "daf_persistence:C++"], 
                     ["ctrl_events", "lsst/ctrl/events/EventLog.h","ctrl_events:C++"],
                     ["python", "Python.h"],
                     ])

pkg = env["eups_product"]
env.libs[pkg] += env.getlibs(" ".join(dependencies))

env.Replace(CXX = 'mpicxx')
# New 
env.Append(INCLUDES = '-DMPICH')
env.Append(CXXFLAGS = "-DMPICH_IGNORE_CXX_SEEK")

for d in Split("doc src lib python/lsst/pex/harness"):
    SConscript(os.path.join(d, "SConscript"))

env['IgnoreFiles'] = r"(~$|\.pyc$|^\.svn$|\.o$)"

Alias("install", [env.Install(env['prefix'], "python"),
                  env.Install(env['prefix'], "include"),
                  env.Install(env['prefix'], "lib"),
                  env.Install(env['prefix'], "bin"),
                  env.InstallAs(os.path.join(env['prefix'], "doc", "doxygen"),
                                os.path.join("doc", "htmlDir")),
                  env.InstallEups(os.path.join(env['prefix'], "ups"),
                                  glob.glob(os.path.join("ups", "*.table")))])

scons.CleanTree(r"*~ core *.so *.os *.o")

env.Declare()
env.Help("""
LSST Distributed Processing  packages
""")
    
