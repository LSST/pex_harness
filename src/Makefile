include installdefs

SHELL = /bin/sh
SUBDIRS = harness

all :: build

.PHONY : build
build : $(SUBDIRS)

.PHONY : $(SUBDIRS)
$(SUBDIRS): 
	$(MAKE) -C $@ -$(MAKEFLAGS) build

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

