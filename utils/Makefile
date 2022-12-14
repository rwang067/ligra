ifdef LONG
INTT = -DLONG
endif

ifdef EDGELONG
INTE = -DEDGELONG
endif

#compilers
ifdef CILK
PCC = g++
PCFLAGS = -std=c++14 -fcilkplus -lcilkrts -O3 -DCILK $(INTT) $(INTE)
PLFLAGS = -fcilkplus -lcilkrts

else ifdef MKLROOT
PCC = icpc
PCFLAGS = -std=c++14 -O3 -DCILKP $(INTT) $(INTE)

else ifdef OPENMP
PCC = g++
PCFLAGS = -std=c++14 -fopenmp -march=native -O3 -DOPENMP $(INTT) $(INTE)

else
PCC = g++
PCFLAGS = -std=c++14 -O3 $(INTT) $(INTE)
endif

COMMON = utils.h parseCommandLine.h parallel.h quickSort.h blockRadixSort.h transpose.h graph.h vertex.h vertexSubset.h index_map.h maybe.h compressedVertex.h byteRLE.h sequence.h
LOCAL_COMMON = graphIO.h
GENERATORS = rMatGraph gridGraph randLocalGraph SNAPtoAdj wghSNAPtoAdj adjGraphAddWeights adjToBinary communityToHyperAdj hyperAdjToBinary adjHypergraphAddWeights randHypergraph KONECTtoHyperAdj KONECTtoClique communityToClique communityToMESH KONECTtoMESH adjToMapData edgeToAdj

.PHONY: all clean
all: $(GENERATORS)

$(COMMON):
	ln -s ../ligra/$@ .

% : %.C $(COMMON) $(LOCAL_COMMON)
	$(PCC) $(PCFLAGS) -o $@ $<

clean :
	rm -f *.o $(GENERATORS) WadjToMapData

cleansrc :
	make -s clean
	rm -f $(COMMON)

WadjToMapData: adjToMapData.C $(COMMON) $(LOCAL_COMMON)
	$(PCC) $(PCFLAGS) -DWEIGHTED -o WadjToMapData adjToMapData.C