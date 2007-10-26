import os, glob, os.path

env = Environment(ENV = os.environ)
Export('env')
dict = env.Dictionary()
ENV  =  dict['ENV']
pwd = ENV['PWD']

pydir = ENV['PYTHON_DIR']
pyincdir =  pydir + "/include/python2.5"
incdir = pwd + "/include"

env.Replace(CPPPATH=incdir + ":" + pyincdir) 

env.Replace(CXX = 'mpicxx')
env.Append(CXXFLAGS = "-DMPICH_IGNORE_CXX_SEEK")

for d in Split("src lib python/lsst/dps/swig"):
    SConscript(os.path.join(d, "SConscript"))

Alias("install", env.Install(pwd + "/bin", pwd + "/test/run.sh"))
Alias("install", env.Install(pwd + "/bin", pwd + "/test/inputImage.fov391.0"))
Alias("install", env.Install(pwd + "/bin", pwd + "/test/nodelist.scr"))
Alias("install", env.Install(pwd + "/bin", pwd + "/test/policy"))

Alias("install", env.Install(pwd + "/bin", pwd + "/test/lsst/dps/test/Mops1EventTransmit.py"))
Alias("install", env.Install(pwd + "/bin", pwd + "/test/lsst/dps/test/AssocIPDPTriggerEventTransmit.py"))
Alias("install", env.Install(pwd + "/bin", pwd + "/test/lsst/dps/test/IpdpImageEventTransmit.py"))

env.Help("""
LSST Distributed Processing  packages
""")
    
