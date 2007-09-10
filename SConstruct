import os, glob, os.path

env = Environment(ENV = os.environ)
Export('env')
dict = env.Dictionary()
ENV  =  dict['ENV']
pwd = ENV['PWD']

# print "pwd is %s " % pwd 

env.Replace(CPPPATH = pwd + "/include")
env.Replace(CXX = 'mpicxx')

for d in Split("src lib python/lsst/dps/swig"):
    SConscript(os.path.join(d, "SConscript"))

Alias("install", env.Install(pwd + "/bin", pwd + "/test/run.sh"))
Alias("install", env.Install(pwd + "/bin", pwd + "/test/nodelist.scr"))
Alias("install", env.Install(pwd + "/bin", pwd + "/test/pipeline.policy"))
Alias("install", env.Install(pwd + "/bin", pwd + "/test/lsst/dps/test/ExternalEventTransmit.py"))

env.Help("""
LSST Distributed Processing  packages
""")
    
