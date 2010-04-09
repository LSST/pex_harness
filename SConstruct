# -*- python -*-
#
# Setup our environment
#
import glob, os.path, re, os
import lsst.SConsUtils as scons

dependencies = "boost utils pex_policy pex_exceptions daf_base pex_logging daf_persistence ctrl_events python".split()

env = scons.makeEnv("pex_harness",
                    r"$HeadURL$",
                    [["boost", "boost/version.hpp", "boost_filesystem:C++"],
                     ["boost", "boost/version.hpp", "boost_system:C++"],
                     ["boost", "boost/regex.hpp", "boost_regex:C++"],
                     ["boost", "boost/serialization/serialization.hpp", "boost_serialization:C++"],
                     ["boost", "boost/serialization/base_object.hpp", "boost_serialization:C++"],
                     ["boost", "boost/test/unit_test.hpp", "boost_unit_test_framework:C++"],                    
                     ["utils", "lsst/utils/Utils.h", "utils:C++"],
                     ["pex_exceptions", "lsst/pex/exceptions.h","pex_exceptions:C++"],
                     ["daf_base", "lsst/daf/base/Citizen.h", "pex_exceptions daf_base:C++"],
                     ["pex_logging", "lsst/pex/logging/Component.h", "pex_logging:C++"],
                     ["pex_policy", "lsst/pex/policy/Policy.h","pex_policy:C++"],
                     ["daf_persistence", "lsst/daf/persistence.h", "daf_persistence:C++"], 
                     ["apr", "apr-1/apr.h", "apr-1"],
                     ["activemqcpp", "activemq/core/ActiveMQConnectionFactory.h"],
                     ["ctrl_events", "lsst/ctrl/events/EventLog.h","ctrl_events:C++"],
                     ["python", "Python.h"],
                     ])

env.Append(LIBPATH = os.path.join(os.environ["ACTIVEMQCPP_DIR"],"lib"))
env.libs["activemqcpp"] += "activemq-cpp".split()
env.libs["activemqcpp"] += env.getlibs("apr")
env.libs["ctrl_events"] += env.getlibs("activemqcpp")

pkg = env["eups_product"]
env.libs[pkg] += env.getlibs(" ".join(dependencies))

#
# Build/install things
#
for d in Split("lib python/lsst/" + re.sub(r'_', "/", pkg) + " tests doc"):
    if os.path.isdir(d):
        SConscript(os.path.join(d, "SConscript"))


env['IgnoreFiles'] = r"(~$|\.pyc$|^\.svn$|\.o$)"

Alias("install", [env.Install(env['prefix'], "python"),
                  env.Install(env['prefix'], "include"),
                  env.Install(env['prefix'], "lib"),
                  env.Install(env['prefix'], "policy"),
                  env.Install(env['prefix'], "bin"),
                  env.Install(env['prefix'], "examples"),
                  env.InstallAs(os.path.join(env['prefix'], "doc", "doxygen"),
                                os.path.join("doc", "htmlDir")),
                  env.InstallEups(os.path.join(env['prefix'], "ups"),
                                  glob.glob(os.path.join("ups", "*.table")))])

scons.CleanTree(r"*~ core *.so *.os *.o")

env.Declare()
env.Help("""
LSST Distributed Processing  packages
""")
    
