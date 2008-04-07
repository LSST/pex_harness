import os, glob, os.path
import lsst.SConsUtils as scons

env = scons.makeEnv("pex_harness",
                    r"$HeadURL$",
                    [["python", "Python.h"],
                     ["boost", "boost/archive/text_oarchive.hpp", "boost_serialization:C++"],
                     ["boost", "boost/version.hpp", "boost_filesystem:C++"],
                     ["boost", "boost/regex.hpp", "boost_regex:C++"],
                     ["seal",  "SealBase/config.h", "lcg_SealBase:C++" ],
                     ["coral", "RelationalAccess/ConnectionService.h", "lcg_RelationalService:C++"],
                     ])


# pydir = ENV['PYTHON_DIR']
# pyincdir =  pydir + "/include/python2.5"
# incdir = pwd + "/include"
# env.Replace(CPPPATH=incdir + ":" + pyincdir) 

env.Replace(CXX = 'mpicxx')
env.Append(CXXFLAGS = "-DMPICH_IGNORE_CXX_SEEK")

for d in Split("doc src lib python/lsst/pex/harness/swig"):
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
    
