# Q8 weather analytics — Hadoop Streaming (C++ binaries)
# Section 2 Q1. The same binaries run under Hadoop Streaming
# (run_hadoop.sh) and in the local pipeline simulation (run_local.sh).

CXX      ?= g++
CXXFLAGS ?= -O2 -std=c++17 -Wall

BIN = build
PROGS = $(BIN)/q8_mapper $(BIN)/q8_combiner $(BIN)/q8_reducer

all: $(PROGS)

$(BIN):
	mkdir -p $(BIN)

$(BIN)/q8_mapper: src/q8_mapper.cpp src/common.h | $(BIN)
	$(CXX) $(CXXFLAGS) -o $@ src/q8_mapper.cpp

$(BIN)/q8_combiner: src/q8_combiner.cpp src/common.h | $(BIN)
	$(CXX) $(CXXFLAGS) -o $@ src/q8_combiner.cpp

$(BIN)/q8_reducer: src/q8_reducer.cpp src/common.h | $(BIN)
	$(CXX) $(CXXFLAGS) -o $@ src/q8_reducer.cpp

test: $(PROGS)
	bash tests/run_tests.sh

clean:
	rm -rf $(BIN)

.PHONY: all test clean
