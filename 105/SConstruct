import os, glob, os.path

env = Environment(ENV = os.environ)
Export('env')
dict = env.Dictionary()
ENV  =  dict['ENV']
pwd = ENV['PWD']

mpichdir = '/home/ncsa/daues/mpich2/install_gcc3.2'

print "pwd is %s " % pwd 

env.Replace(CPPPATH = mpichdir + "/include:" + pwd + "/include")
env.Replace(CXX = 'mpicxx')

for d in Split("src lib python/lsst/dps/swig"):
    SConscript(os.path.join(d, "SConscript"))


Alias("install", env.Install(pwd + "/bin", pwd + "/conf/run.sh"))
Alias("install", env.Install(pwd + "/bin", pwd + "/conf/nodelist.scr"))
Alias("install", env.Install(pwd + "/bin", pwd + "/conf/pipeline.policy"))

env.Help("""
LSST Distributed Processing  packages
""")
    
