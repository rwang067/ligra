export OPENMP := 1
export LONG := 1

ifdef SPDK
PKG_CONFIG_PATH = $(SPDK_DIR)/build/lib/pkgconfig
SPDK_LIB := -pthread -Wl,--no-as-needed
# PCFLAGS += -Wl,--no-as-needed -ldl -lpthread -lrt -lssl -lcrypto
SPDK_LIB += $(shell PKG_CONFIG_PATH="$(PKG_CONFIG_PATH)" pkg-config --libs spdk_nvme spdk_env_dpdk)
SPDK_LIB += -Wl,--as-needed -lisal -lisal_crypto

# ISAL_DIR=$(SPDK_DIR)/isa-l
# ISAL_CRYPTO_DIR=$(SPDK_DIR)/isa-l-crypto
# ISAL_BUILD_DIR=$(SPDK_DIR)/isalbuild

# SPDK_LIB += $(ISAL_DIR)/.libs/libisal.a
# SPDK_LIB += $(ISAL_CRYPTO_DIR)/.libs/libisal_crypto.a

endif

ifdef CHUNK
CK = -DCHUNK
endif

ifdef DISKIO
DI = -DDISKIO
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
PCFLAGS = -std=c++14 -fcilkplus -lcilkrts -O3 -DCILK $(INTT) $(INTE) $(CODE) $(PD) $(MEM) $(CK) $(DI) $(CM) $(PCM)

else ifdef CILKP
PLFLAGS = -fcilkplus -lcilkrts

else ifdef MKLROOT
PCC = icpc
PCFLAGS = -std=c++14 -O3 -DCILKP $(INTT) $(INTE) $(CODE) $(PD) $(MEM) $(CK) $(DI) $(CM) $(PCM)

else ifdef OPENMP
PCC = g++
PCFLAGS = -std=c++14 -fopenmp -march=native -O3 -DOPENMP $(INTT) $(INTE) $(CODE) $(PD) $(MEM) $(CK) $(DI) $(CM) $(PCM)

else
PCC = g++
PCFLAGS = -std=c++14 -O3 -g $(INTT) $(INTE) $(CODE) $(PD) $(MEM) $(CK) $(DI) $(CM) $(PCM)
endif

# ifdef SPDK
# PCFLAGS += $(PKG_CONFIG_PATH="$(PKG_CONFIG_PATH)" pkg-config --cflags libdpdk)
# endif

COMMON= ligra.h graph.h chunk_buffer.h compressedVertex.h vertex.h utils.h IO.h parallel.h gettime.h index_map.h maybe.h sequence.h edgeMap_utils.h binary_search.h quickSort.h blockRadixSort.h transpose.h parseCommandLine.h byte.h byteRLE.h nibble.h byte-pd.h byteRLE-pd.h nibble-pd.h vertexSubset.h encoder.C decoder.C monitor.h spdk.h

ALL= encoder decoder BFS BC Components Components-Shortcut Radii PageRank PageRankDelta BFSCC BFS-Bitvector KCore TestAll testNebrs BellmanFord MIS CF Triangle

all: $(ALL)

% : %.C $(COMMON)
	$(PCC) $(PCFLAGS) -o $@ $< $(SPDK_LIB)

$(COMMON):
	ln -s ../ligra/$@ .

.PHONY : clean

clean :
	rm -f *.o $(ALL)

cleansrc :
	rm -f *.o $(ALL)
	rm $(COMMON)
