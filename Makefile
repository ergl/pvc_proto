PACKAGE         ?= rubis_proto
VERSION         ?= $(shell git describe --tags)
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR            = $(shell pwd)/rebar3
MAKE						 = make

.PHONY: gen devdep rel deps test eqc plots

all: compile

gen: devdep
	_build/dev/lib/gpb/bin/protoc-erl -strbin -maps -I. proto/*.proto -o src/

##
## Compilation targets
##

devdep:
	$(REBAR) as dev compile

compile:
	$(REBAR) compile

clean: packageclean
	$(REBAR) clean

packageclean:
	rm -fr *.deb
	rm -fr *.tar.gz

##
## Test targets
##

check: xref dialyzer lint

lint:
	${REBAR} as lint lint

shell:
	${REBAR} shell --apps rubis_proto

include tools.mk
