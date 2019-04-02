RM = rm -f
CC = g++
CFLAGS = $(shell pkg-config --cflags seastar) -g -O0
LDFLAGS = $(shell pkg-config --libs --static seastar) 

SOURCES = ../../src/common/Status.cpp Main.cpp  AssignmentManagerTest.cpp

INC = -I../../src -I../include

OBJECTS=$(SOURCES:.cpp=.o)
EXECUTABLE=node_test

all: $(EXECUTABLE)

$(EXECUTABLE): $(OBJECTS)
	$(CC) $(OBJECTS) $(LDFLAGS)  -o $(EXECUTABLE)

%.o : %.cpp
	$(CC) $(CFLAGS) $(INC) -c $< -o $@

clean:
	-$(RM) $(OBJECTS)
	-$(RM) $(EXECUTABLE)

.PHONY: all clean dependents
.SECONDARY:
