# TEST_VERBOSE:=1
# TEST_VERBOSE:=

V=$(if $(findstring 1,$(TEST_VERBOSE)),-v)

test:
	@go test $V -count=1 -coverprofile cover.out ./
	@go tool cover -html=cover.out -o coverage.html
	@echo Coverage report in file://$$PWD/coverage.html

doc:
	pkgsite -http=127.0.0.1:6060
