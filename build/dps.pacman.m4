#
#   package version
#
#
m4_changequote([, ])m4_dnl
#
m4_dnl
m4_dnl  For a simple external package that follows the configure-make pattern,
m4_dnl  it may only be necessary to update the values of the following macros.
m4_dnl  Only m4_PACKAGE and m4_VERSION are required.  
m4_dnl
m4_define([m4_PACKAGE], [dps])m4_dnl
m4_define([m4_VERSION], [0.3])m4_dnl
m4_define([m4_TARBALL], [m4_PACKAGE-m4_VERSION.tar.gz])m4_dnl
# 
# set up the initial pacman definitions and environment variables.
#
m4_include([PacmanLsst-pre.m4])m4_dnl
m4_dnl
m4_dnl  uncomment and adjust freeMegsMinimum() if you know a good value
m4_dnl  for this package.
m4_dnl
# freeMegsMinimum(11)       # requires at least 11 Megs to build and install

#
# denote dependencies
#
# package('m4_CACHE:otherpkg-2.2')
setenvShellTemp('PYTHON_DIR', 'export SHELL=sh; source $EUPS_DIR/bin/setups.sh; setup Python; echo $PYTHON_DIR')
envIsSet('PYTHON_DIR')
echo('Using PYTHON_DIR=$PYTHON_DIR')
shell('[[ -d "$PYTHON_DIR" ]]')

setenvShellTemp('LAM_DIR', 'export SHELL=sh; source $EUPS_DIR/bin/setups.sh; setup LAM; echo $LAM_DIR')
envIsSet('LAM_DIR')
echo('Using LAM_DIR=$LAM_DIR')
shell('[[ -d "$LAM_DIR" ]]')

setenvShellTemp('NETLOGGER_DIR', 'export SHELL=sh; source $EUPS_DIR/bin/setups.sh; setup NetLogger; echo $NETLOGGER_DIR')
envIsSet('NETLOGGER_DIR')
echo('Using NETLOGGER_DIR=$NETLOGGER_DIR')
shell('[[ -d "$NETLOGGER_DIR" ]]')

#
# begin installation assuming we are located in LSST_HOME
#
# available environment variables:
#   LSST_HOME           the root of the LSST installation (the current 
#                          directory)
#   LSST_BUILD          a directory where one can build the package
#
# EUPS_PATH and EUPS_FLAVOR should also be set.
#

cd('$LSST_BUILD')

#
#   download any tarballs and unzip
#
echo ("downloading and extracting m4_PACKAGE-m4_VERSION...")
downloadUntar('m4_PKGURL/m4_PKGPATH/m4_TARBALL','BUILDDIR')

#
#   cd into the untarred directory, configure, make and make install
#
cd('$BUILDDIR')
echo ("configuring m4_PACKAGE-m4_VERSION...")
shell('export SHELL=sh; source $EUPS_DIR/bin/setups.sh; unsetup m4_PACKAGE 2&>1 /dev/null; setup Python; setup LAM; setup NetLogger; ./configure')

echo ("running make install")
shell('export SHELL=sh; source $EUPS_DIR/bin/setups.sh; unsetup m4_PACKAGE 2&>1 /dev/null; setup Python; setup LAM; setup NetLogger; make installnowarn')
cd()

#
# Now download & install the EUPS table file and load package into EUPS
#
m4_include([PacmanLsst-post.m4])m4_dnl

uninstallShell('rm -rf $PWD/m4_PACKAGE/m4_VERSION')
uninstallShell('rmdir $PWD/m4_PACKAGE; true')



