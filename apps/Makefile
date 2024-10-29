export OPENMP := 1
export LONG := 1

ifdef CHUNK
CK = -DCHUNK
endif

ifdef CACHEMISS
CM = -DCACHEMISS
endif

ifdef PAGECACHE
PCM = -DPAGECACHE
endif

ifdef LONG
INTT = -DLONG
endif

ifdef EDGELONG
INTE = -DEDGELONG
endif

ifdef PD
PD = -DPD
endif

ifdef BYTE
CODE = -DBYTE
else ifdef NIBBLE
CODE = -DNIBBLE
else
CODE = -DBYTERLE
endif

ifdef LOWMEM
MEM = -DLOWMEM
endif

#compilers
ifdef CILK
PCC = g++
PCFLAGS = -std=c++14 -fcilkplus -lcilkrts -O3 -DCILK $(INTT) $(INTE) $(CODE) $(PD) $(MEM) $(CK) $(CM) $(PCM)

else ifdef CILKP
PLFLAGS = -fcilkplus -lcilkrts

else ifdef MKLROOT
PCC = icpc
PCFLAGS = -std=c++14 -O3 -DCILKP $(INTT) $(INTE) $(CODE) $(PD) $(MEM) $(CK) $(CM) $(PCM)

else ifdef OPENMP
PCC = g++
# PCFLAGS = -std=c++14 -fopenmp -march=native -O0 -DOPENMP $(INTT) $(INTE) $(CODE) $(PD) $(MEM) $(CK) $(CM) $(PCM)
PCFLAGS = -std=c++14 -fopenmp -march=native -O3 -DOPENMP $(INTT) $(INTE) $(CODE) $(PD) $(MEM) $(CK) $(CM) $(PCM)

else
PCC = g++
# PCFLAGS = -std=c++14 -O0 -g $(INTT) $(INTE) $(CODE) $(PD) $(MEM) $(CK) $(CM) $(PCM)
PCFLAGS = -std=c++14 -O3 -g $(INTT) $(INTE) $(CODE) $(PD) $(MEM) $(CK) $(CM) $(PCM)
endif

COMMON= ligra.h graph.h chunk_buffer.h compressedVertex.h vertex.h utils.h IO.h parallel.h gettime.h index_map.h maybe.h sequence.h edgeMap_utils.h binary_search.h quickSort.h blockRadixSort.h transpose.h parseCommandLine.h byte.h byteRLE.h nibble.h byte-pd.h byteRLE-pd.h nibble-pd.h vertexSubset.h encoder.C decoder.C monitor.h

ALL= encoder decoder BFS BC Components Components-Shortcut Radii PageRank PageRankDelta BFSCC BFS-Bitvector KCore TestAll testNebrs BellmanFord MIS CF Triangle

all: $(ALL)

% : %.C $(COMMON)
	$(PCC) $(PCFLAGS) -o $@ $<

$(COMMON):
	ln -s ../ligra/$@ .

.PHONY : clean

clean :
	rm -f *.o $(ALL)

cleansrc :
	rm -f *.o $(ALL)
	rm $(COMMON)
