# TEST_VERBOSE:=1
# TEST_VERBOSE:=
# TEST_RUN:=...

# adjust as necessary
export PGLOGREPLSIMPLE_TEST_CONNINFO=postgres://postgres:pwp@127.0.0.1:5440/test?options=-clog_min_duration_statement=0

V=$(if $(findstring 1,$(TEST_VERBOSE)),-v)
R=$(if $(TEST_RUN),-run $(TEST_RUN))

all: test example/example
.PHONY: all

example/example: simple.go recv.go example/main.go
	go build -o example ./example

test:
	@go test $V $R -count=1 -coverprofile cover.out ./
	@go tool cover -html=cover.out -o coverage.html
	@echo Coverage report in file://$$PWD/coverage.html

doc:
	pkgsite -http=127.0.0.1:6060
