import os, glob, os.path
import lsst.SConsUtils as scons

env = scons.makeEnv("dps",
                    r"$HeadURL$",
                    [["python", "Python.h"],
                     ["boost", "boost/archive/text_oarchive.hpp", "boost_serialization:C++"],
                     ["boost", "boost/version.hpp", "boost_filesystem:C++"],
                     ["boost", "boost/regex.hpp", "boost_regex:C++"],
                     ["seal",  "SealBase/config.h", "lcg_SealBase:C++" ],
                     ["coral", "RelationalAccess/ConnectionService.h", "lcg_RelationalService:C++"],
                     ["mwi", "lsst/mwi/data.h", "boost_filesystem boost_regex boost_serialization lcg_RelationalService lcg_SealBase mwi:C++"]
                     ])


# pydir = ENV['PYTHON_DIR']
# pyincdir =  pydir + "/include/python2.5"
# incdir = pwd + "/include"
# env.Replace(CPPPATH=incdir + ":" + pyincdir) 

env.Replace(CXX = 'mpicxx')
env.Append(CXXFLAGS = "-DMPICH_IGNORE_CXX_SEEK")

for d in Split("src lib python/lsst/dps/swig"):
    SConscript(os.path.join(d, "SConscript"))

env.Help("""
LSST Distributed Processing  packages
""")
    
