# Template Makefile for simple directories requiring no compilations 
# (like etc and scripts).  Typically for such directories, make needs
# only to worry about trivially copying the files to the installation 
# area; however, this Makefile can be easily adapted to recurse into
# sub-directories or "make" files using m4.
#
# If you wish to run make inside this directory be sure to pass
# "-I <topdir>/build" as arguments to make so that this include file 
# will be found.
#
include installdefs
SHELL = /bin/sh

# set this to directory will files will be installed into.
#
IDIR = $(DOCDIR)

# set this to any subdirectories where make must run recursively.
# These must contain there own Makefiles.  If copying (or m4-ing) is
# all that is necessary to do in a subdirectory, you can leave this
# blank. 
#
SUBDIRS = 

# set this to the list of files that just need to be trivially copied.
# The default will find all files (except Makefiles, m4 files or .svn
# directories).  
#
SIMPLEFILES := $(shell find . \( -name .svn -prune -o -type f \! -name Makefile \! -name \*.m4 \! -name \*~ \! -name \*.bak -print \))

# set this to the list of files that are built by processing templates
# (ending with .m4) through m4. 
# The default will find all files (except Makefiles, m4 files or .svn
# directories).  
#
M4FILES = $(shell find . -name \*.m4 -print | sed -e 's/\.m4//')

# define the arguments for m4
#
M4FLAGS =

FILES := $(SIMPLEFILES) $(M4FILES)

# list the executable files.  If this is for the script directory, then
# this is usually everything we find.  
#
# EXEFILES = $(FILES)
EXEFILES =

EXEMODE = u+rwx,go+rx

all : build

.PHONY : build
build : $(EXEFILES) $(FILES) $(SUBDIRS) 
	$(if $(EXEFILES), chmod $(EXEMODE) $(EXEFILES))

.PHONY : $(SUBDIRS)
$(SUBDIRS): 
	$(MAKE) -C $@ -$(MAKEFLAGS) build

.PHONY : simpleinstall
simpleinstall : $(FILES) build 
	install -d $(IDIR)
	@$(if $(FILES),                                \
           echo simple taring over of files:;          \
	   tar cf - $(FILES) | (cd $(IDIR); tar xvf -))

.PHONY : install
install :: simpleinstall

$(M4FILES) : $@.m4
	m4 $(M4FLAGS) $@.m4 > $@

.PHONY : build
install clean distclean ::
	@for f in $(SUBDIRS); do \
		if [ ! -d $$f ]; then \
			echo No such directory: $$f >&2; \
		else \
			if [ ! \( $$f = test -a $@ = all \) ]; then \
				(cd $$f; echo $$f; $(MAKE) -$(MAKEFLAGS) $@); \
			fi; \
		fi \
	done

.PHONY: distclean
distclean :: clean

#
# Update Makefile dependencies
#
.PHONY: depend
depend :
	@for f in swig; do \
		(cd $$f; echo $$f; $(MAKE) $(MAKEFLAGS) $@); \
	done

