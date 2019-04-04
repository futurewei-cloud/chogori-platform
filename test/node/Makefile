RM = rm -f
CC = g++
CFLAGS = $(shell pkg-config --cflags seastar) -g3
CXXFLAG = $(CFLAGS) -std=c++17 -march=broadwell -mavx -mavx2 -mbmi2 -mlzcnt -Wno-deprecated-declarations -Wall -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new -fstack-protector-all -Wno-sign-compare -Wno-class-memaccess 
LDFLAGS = $(shell pkg-config --libs --static seastar) 

SOURCES = ../../src/common/Status.cpp Main.cpp  AssignmentManagerTest.cpp

INC = -I../../src -I../include -I$(HOME)/projects/DFV_K2_HOT/libs/hot/commons/include/ -I$(HOME)/projects/DFV_K2_HOT/libs/idx/content-helpers/include/ -I$(HOME)/projects/DFV_K2_HOT/libs/hot/single-threaded/include/

OBJECTS=$(SOURCES:.cpp=.o)
EXECUTABLE=node_test

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