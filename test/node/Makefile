CC=g++
CFLAGS := $(shell pkg-config --libs --cflags --static seastar) -g -O0
LDFLAGS=
SOURCES=*.cpp ../../src/common/Status.cpp
INC=-I../../src
#-I../../../seastar/include
#HEADERS=../*.h
OBJECTS=$(SOURCES:.cpp=.o)
EXECUTABLE=node_test

all: $(SOURCES) $(EXECUTABLE)

$(EXECUTABLE): $(SOURCES)
	$(CC) $(SOURCES) $(CFLAGS) $(INC) -o $@
#	clear && printf "\n\n\n\n\n\n\n" && $(CC) -std=c++17 -fmax-errors=1 $(SOURCES) $(INC) -o $@
#	clear && printf "\n\n\n\n\n\n\n" && $(CC) -std=c++17 $(SOURCES) $(INC) -o $@
