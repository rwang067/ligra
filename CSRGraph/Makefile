INCFLAGS = -I./src/config/ -I./apps/ 

CPP = g++
CPPFLAGS = -g -O3 $(INCFLAGS)  -fopenmp -Wall -Wno-strict-aliasing  -Wno-unknown-pragmas
# CPPFLAGS = -g -O0 $(INCFLAGS)  -fopenmp -Wall -Wno-strict-aliasing  -Wno-unknown-pragmas
LINKERFLAGS = -lz -lnuma
DEBUGFLAGS = -g -ggdb $(INCFLAGS)
HEADERS=$(shell find . -name '*.hpp')

all : clean main

clean:
	@rm -rf bin/

main: main.cpp $(HEADERS)
	@mkdir -p bin/
	$(CPP) $(CPPFLAGS) main.cpp -o bin/main $(LINKERFLAGS)
