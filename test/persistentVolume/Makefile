RM = rm -f
CC = g++
CFLAGS = $(shell pkg-config --cflags seastar) -g
CXXFLAG = $(CFLAGS) -std=c++17 
LDFLAGS_SEASTAR = $(shell pkg-config --libs --static seastar) 
LDFLAGS = -lseastar_testing -lboost_unit_test_framework -lstdc++fs $(LDFLAGS_SEASTAR) 

SOURCES = ../../src/common/Status.cpp ../../src/common/plog/PlogMock.cpp ../../src/node/persistence/PersistentVolume.cpp PersistentVolumeTest.cpp Main.cpp

INC = -I../../src -I../include 

OBJECTS=$(SOURCES:.cpp=.o)
EXECUTABLE=persistentVolume_test

all: $(EXECUTABLE)

$(EXECUTABLE): $(OBJECTS)
	$(CC) $(OBJECTS) $(LDFLAGS)  -o $(EXECUTABLE)

%.o : %.cpp
	$(CC) $(CXXFLAG) $(INC) -c $< -o $@

clean:
	-$(RM) $(OBJECTS)
	-$(RM) $(EXECUTABLE)

.PHONY: all clean dependents
.SECONDARY: