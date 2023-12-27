#pragma once
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <mutex>
#include <stdlib.h>
#include <sys/mman.h>
#include <asm/mman.h>
#include <cassert>
#include <omp.h>
#include "parallel.h"
#include "monitor.h"

using namespace std;

#define MAP_HUGE_2MB (21 << MAP_HUGE_SHIFT)

inline void lock(volatile bool &flag)
{
	while(!__sync_bool_compare_and_swap(&flag, 0, 1)){}
}
inline void unlock(volatile bool &flag)
{
	flag = 0;
}

template <typename T>
void preada(int f, T * tbuf, size_t nbytes, size_t off = 0) {
    size_t nread = 0;
    T * buf = (T*)tbuf;
    while(nread<nbytes) {
        ssize_t a = pread(f, buf, nbytes - nread, off + nread);
        if (a == (-1)) {
            std::cout << "Error, could not read: " << strerror(errno) << "; file-desc: " << f << std::endl;
            std::cout << "Pread arguments: " << f << " tbuf: " << tbuf << " nbytes: " << nbytes << " off: " << off << std::endl;
            exit(-1);
        }
        buf += a/sizeof(T);
        nread += a;
    }
    assert(nread <= nbytes);
}

char* getFileData(const char* filename, size_t size = 0, bool isMmap = 0, bool isProfile = 0){
  char* addr = 0;
  if(!isMmap){
    ifstream in(filename,ifstream::in | ios::binary); //stored as uints
    in.seekg(0, ios::end);
    long size1 = in.tellg();
    in.seekg(0);
    if(size1 != size){ 
      cout << size1 << " " << size << std::endl;
      cout << "Filename size wrong for :" << filename << std::endl; 
      cout << "Specified size = " << size << ", read size = " << size1 << std::endl;
      abort(); 
    }
    addr = (char *) malloc(size);
    in.read(addr,size);
    in.close();
  } else {
    int fd = open(filename, O_RDWR|O_CREAT, 00777);
    if(fd == -1){
      std::cout << "Could not open file for :" << filename << " error: " << strerror(errno) << std::endl;
      exit(1);
    }
    if(ftruncate(fd, size) == -1){
      std::cout << "Could not ftruncate file for :" << filename << " error: " << strerror(errno) << std::endl;
      close(fd);
      exit(1);
    }
    // addr = (char*)mmap(NULL, size, PROT_READ|PROT_WRITE,MAP_SHARED, fd, 0);
    addr = (char*)mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
    if(addr == MAP_FAILED) {	
      std::cout << "Could not mmap file for :" << filename << " error: " << strerror(errno) << std::endl;
      close(fd);
      exit(1);
    } else {
      std::cout << "mmap succeeded, size = " << B2GB(size) << "GB, filename = " << filename << std::endl;
      std::cout << "size = " << size << ", addr = " << (void*)addr << std::endl;
    }
    // if (isProfile) {
    //   preada(fd, addr, size, 0);
    // }
  }
  return addr;
}

typedef uint32_t cid_t; //chunk_id
typedef uint32_t hot_t; //chunk_id
struct Chunk_t{
  hot_t hotness;
  uint16_t max_size;
  uint16_t cur_size;
};

class ChunkBuffer{
private:
  size_t chunk_size;
  cid_t nchunks, nmchunks; 

  int cfd; 
  void* buff; 
  char** mchunks;
  cid_t cur_mcid;
  bool is_huge_pages;

  cid_t *cmap, *mcmap; // cid -> mcid, mcid -> cid
  volatile bool* chunk_lock;

  hot_t *hotness;
  hot_t hotsum;
  
  cid_t loaded_chunk_count, freed_chunk_count;
  size_t space_waste;

  long job;
  bool update;

public:
  ChunkBuffer(const char* filename, size_t _chunk_size, cid_t _nchunks, cid_t _nmchunks, long job, bool update)
  :chunk_size(_chunk_size), nchunks(_nchunks), nmchunks(_nmchunks), job(job), update(update) {
    // cfd = open(filename, O_RDONLY);// | O_DIRECT); //| O_NOATIME);
    cfd = open(filename, O_RDONLY | O_DIRECT | O_NOATIME);
    if(cfd == -1)
    {
      fprintf(stdout,"Wrong open %s\n",filename);
      perror("open");
      exit(-1);
	  }
    cout << "Open file " << filename << ", cfd = " << cfd << endl;

    buff = 0;
    if(chunk_size >= 2097152){
      is_huge_pages = 1;
      buff = (char *)mmap(NULL, nmchunks*chunk_size, PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_HUGE_2MB, 0, 0);
      if(buff == MAP_FAILED) {	
        perror("mmap");
        buff = 0;
      }
    } 
    if(buff == 0) {
      is_huge_pages = 0;
      buff = (char*)calloc(nmchunks, chunk_size);
    }

    int ret;
    /* allocate 1 KB along a 256-byte boundary */
    // ret = posix_memalign (&buf, 256, 1024);
    ret = posix_memalign (&buff, chunk_size, nmchunks*chunk_size);
    if (ret) {
      fprintf (stderr, "posix_memalign: %s\n", strerror (ret));
      exit(-1);
    }
    memset(buff, 0, nmchunks*chunk_size);

    mchunks = (char**)calloc(nmchunks, sizeof(char*));
    for(int i = 0; i < nmchunks; i++){
      mchunks[i] = (char*)buff + i * chunk_size;
    }
    cur_mcid = 0;

    cmap = (cid_t*)calloc(nchunks, sizeof(cid_t));
    for(int i = 0; i < nchunks; i++){
      cmap[i] = nmchunks; 
    }
    mcmap = (cid_t*)calloc(nmchunks, sizeof(cid_t));
    for(int i = 0; i < nmchunks; i++){
      mcmap[i] = nchunks; 
    }

    cout << "buff addr = " << (void*)buff << ", buff size = " << (nmchunks * chunk_size /1024/1024) << " MB." << endl;
    // cout << "mchunks addrs = " << endl;
    // for(int i = 0; i < nmchunks; i++){
    //   cout << (void*)(mchunks[i]) << " ";
    // }
    // cout << endl;

    chunk_lock = (volatile bool*)calloc(nchunks, sizeof(volatile bool));

    hotness = (hot_t*)calloc(nmchunks, sizeof(hot_t));
    hotsum = 0;

    loaded_chunk_count = 0;
    freed_chunk_count = 0;
    space_waste = 0;

    pre_load();

    cout << "ChunkBuffer initialized " << nmchunks << " mchunks of size " << chunk_size << ", nchunks = " << nchunks << endl;
  }
  ~ChunkBuffer(){
  }

  void pre_load(){
    // cid_t mcid = 0;
    // for(cid_t cid = 0; cid < nmchunks; cid++){
    //   mcid = evict_colder(hotsum/nmchunks);
    //   load_chunk(cid, mcid);
    // }
    load_chunks(0,0,nmchunks);
    cout << "ChunkBuffer pre_loaded the former " << nmchunks << " chunks." << endl;
  }

  void del(){
    { // used for debug, check the correctness of cmap and mcmap
      int mcached_count = 0;
      for(int i = 0; i < nmchunks; i++){
        if(mcmap[i] != nchunks){
          mcached_count++;
          if(cmap[mcmap[i]] != i)
            cout << "Error cmap of chunksize_" << chunk_size << " : i mcmap[i] cmap[mcmap[i]] mcmap[cmap[mcmap[i]]] = " << i << ", " << mcmap[i] << ", " << cmap[mcmap[i]] << ", " << mcmap[cmap[mcmap[i]]] << endl;
        }
      }

      int cached_count = 0;
      for(int i = 0; i < nchunks; i++){
        if(cmap[i] != nmchunks){
          cached_count++;
          if(mcmap[cmap[i]] != i)
            cout << "Error mcmap of chunksize_" << chunk_size << " : i cmap[i] mcmap[cmap[i]] cmap[mcmap[cmap[i]]] = " << i << ", " << cmap[i] << ", " << mcmap[cmap[i]] << ", " << cmap[mcmap[cmap[i]]] << endl;
        }
      }

      cout << "mcmap cached chunk count = " << mcached_count << endl;
      cout << "cmap cached chunk count = " << cached_count << ", hotness sum = " << hotsum << ", hotness avg = " << hotsum/nmchunks << endl;
      // cout << "loaded chunk count = " << loaded_chunk_count << ", wasted space = " << space_waste << ", avg = " << space_waste/loaded_chunk_count<< endl;
      // cout << "freed chunk count = " << freed_chunk_count << ", loaded-freed chunk count = " << (loaded_chunk_count-freed_chunk_count) << endl;
    }

    close(cfd);
    cout << "Close file, cfd = " << cfd << ".\n" << endl;
    free(mcmap);
    free(cmap);
    free(mchunks);

    if(is_huge_pages) munmap(buff, chunk_size*nmchunks);
    else free(buff);
    // free(chunk_lock);
  }

  cid_t get_cmap(cid_t cid){
    return cmap[cid];
  }
  cid_t get_mcmap(cid_t mcid){
    return mcmap[mcid];
  }

  cid_t evict_seq(){
    return __sync_fetch_and_add(&cur_mcid, 1)%nmchunks;
  }
  cid_t evict_rand(){
    return rand()%nmchunks;
  }
  cid_t evict_colder(hot_t threshold){
    cid_t res_cid = __sync_fetch_and_add(&cur_mcid, 1)%nmchunks;
    while(hotness[res_cid]>threshold){
      res_cid = __sync_fetch_and_add(&cur_mcid, 1)%nmchunks;
    }
    return res_cid;
  }
  cid_t evict_colder_decay(hot_t threshold){
    cid_t res_cid = __sync_fetch_and_add(&cur_mcid, 1)%nmchunks;
    while(hotness[res_cid]>threshold){
      hotsum -= hotness[res_cid]/2;
      hotness[res_cid] /= 2;
      res_cid = __sync_fetch_and_add(&cur_mcid, 1)%nmchunks;
    }
    return res_cid;
  }
  cid_t evict_coldest(){
    cid_t res_cid = cur_mcid;
    hot_t res_hot = hotness[res_cid];
    for(cid_t i = cur_mcid+1; i < nmchunks; i++){
      if(res_hot < hotness[i]){
        res_cid = i;
        res_hot = hotness[res_cid];
      }
    }
    for(cid_t i = 0; i < cur_mcid; i++){
      if(res_hot < hotness[i]){
        res_cid = i;
        res_hot = hotness[res_cid];
      }
    }
    hotness[res_cid] = 0;
    cur_mcid = res_cid;
    return res_cid;
  }

  uintE* get_nebrs_from_mchunk(cid_t cid, uint32_t coff, uint32_t d){
    char* mchunk = get_mchunk(cid);
    if (update) update_mchunk_hot(cmap[cid], sizeof(uintE)*d);
    return (uintE*)(mchunk+coff);
  }

  char* get_mchunk(cid_t cid){
#ifdef PROFILE_EN
  int tid = getWorkersID();
  profiler.profile_get_chunk(tid);
#endif
    if(cmap[cid] == nmchunks){ // Not in DRAM buffer
      while(chunk_lock[cid]);
      if(cmap[cid] == nmchunks) { 
        while(chunk_lock[cid]);
        lock(chunk_lock[cid]);
        if(cmap[cid] == nmchunks) {  
          cid_t mcid;
          switch (job)
          {
          case 0:
            mcid = evict_seq();
            break;
          case 1:
            mcid = evict_rand();
            break;
          case 2:
            mcid = evict_colder(hotsum/nmchunks);
            break;
          case 3:
            mcid = evict_colder_decay(hotsum/nmchunks*2);
            break;
          case 4:
            mcid = evict_coldest();
            break;
          default:
            std::cerr << "Error job = " << job << endl;
            break;
          }
          cid_t mmcid = mcmap[mcid];
          if(mmcid != nchunks){
            free_chunk(mmcid, mcid);
          }
          load_chunk(cid, mcid);
        }
        unlock(chunk_lock[cid]);
      }
    }
    return mchunks[cmap[cid]];
  }

  void load_chunk(cid_t cid, cid_t mcid){
#ifdef PROFILE_EN
  int tid = getWorkersID();
  profiler.profile_load_chunk(tid);
#endif
    preada(cfd,mchunks[mcid],chunk_size,chunk_size*cid);
    cmap[cid] = mcid;
    mcmap[mcid] = cid;

    hotness[mcid] = ((Chunk_t*)(mchunks[mcid]))->hotness;
    hotsum += hotness[mcid]; // __sync_fetch_and_add(&hotsum, hotness[mcid]);

    // __sync_fetch_and_add(&loaded_chunk_count, 1);
    // uint16_t max_size = ((Chunk_t*)(mchunks[mcid]))->max_size;
    // uint16_t cur_size = ((Chunk_t*)(mchunks[mcid]))->cur_size;
    // __sync_fetch_and_add(&space_waste, max_size - cur_size);
  }
  void load_chunks(cid_t cid, cid_t mcid, cid_t count){ // read `count` chunks together
    preada(cfd,mchunks[mcid],chunk_size*count,chunk_size*cid);
    for(cid_t id = 0; id < count; id++){
      cmap[cid+id] = mcid+id;
      mcmap[mcid+id] = cid+id;
      hotness[mcid+id] = ((Chunk_t*)(mchunks[mcid+id]))->hotness;
      hotsum += hotness[mcid+id]; // __sync_fetch_and_add(&hotsum, hotness[mcid]);
    }
  }
  void free_chunk(cid_t mmcid, cid_t mcid){
    hotsum -= hotness[mcid]; // __sync_fetch_and_sub(&hotsum, hotness[mcid]);
    cmap[mmcid] = nmchunks;
    mcmap[mcid] = nchunks;

    // __sync_fetch_and_add(&freed_chunk_count, 1);
  }

  void update_mchunk_hot(cid_t mcid, hot_t h){
    hotness[mcid] += h; // __sync_fetch_and_add(&hotness[mcid], h);
    hotsum += h; // __sync_fetch_and_add(&hotsum, h);
  }
  void update_chunk_hot(cid_t cid, hot_t h){
    cid_t mcid = cmap[cid];
    if(mcid != nmchunks){
      update_mchunk_hot(mcid, h);
    }
  }
}; 

#define HUGE_PAGE_SIZE 2097152 // 2MB
#define DIRECT_GRAPH 2

struct TriLevelReader {
  long n, m, level;
  std::string chunkFile, rchunkFile;
  long *end_deg, *chunk_sz, *nchunks;
  long *rend_deg, *rchunk_sz, *rnchunks;
  std::string svFile, rsvFile;
  long sv_size, rsv_size;
  std::string configFile;
  std::string vertFile;
  std::string reorderListFile;

  void readConfig(char* iFile, bool debug = false) {
    string baseFile = iFile;
    configFile = baseFile + ".config";
    vertFile = baseFile + ".vertex";
    chunkFile = baseFile + ".adj.chunk";
    rchunkFile = baseFile + ".radj.chunk";
    svFile = baseFile + ".adj.sv";
    rsvFile = baseFile + ".radj.sv";
    reorderListFile = baseFile + ".reorder";

    ifstream in(configFile.c_str(), ifstream::in);
    in >> n >> m >> level;

    end_deg = new long[level];
    chunk_sz = new long[level];
    nchunks = new long[level];
    
    in >> sv_size;
    for (int i = 0; i < level; i++) {
      in >> end_deg[i] >> chunk_sz[i] >> nchunks[i];
    }
    
    rend_deg = new long[level];
    rchunk_sz = new long[level];
    rnchunks = new long[level];
    
    in >> rsv_size;
    for (int i = 0; i < level; i++) {
      in >> rend_deg[i] >> rchunk_sz[i] >> rnchunks[i];
    }
    in.close();

    if (debug) {
      cout << "ConfigFile: " << configFile << endl; 
      cout << "n = " << n << ", m = " << m << ", level = " << level << endl;
      cout << "sv_size = " << sv_size << endl;
      for (int i = 0; i < level; i++)
        std::cout << "level = " << i << ", end_deg = " << end_deg[i] << ", chunk_sz = " << chunk_sz[i] << ", nchunks = " << nchunks[i] << std::endl;
      cout << "rsv_size = " << rsv_size << endl;
      for (int i = 0; i < level; i++)
        std::cout << "level = " << i << ", rend_deg = " << rend_deg[i] << ", rchunk_sz = " << rchunk_sz[i] << ", rnchunks = " << rnchunks[i] << std::endl;
    }
  }
};

class ChunkManager {
private:
  std::string chunkFile;
  long chunk_sz = 0;
  long nchunks = 0;
  long level = 0;
  void* addr = 0;
  long nbuff = 0;
  int chunk_fd = 0;

public:
  ChunkManager() {}
  ChunkManager(std::string iFile, long s, long n, long l) : chunkFile(iFile), chunk_sz(s), nchunks(n), level(l) {}

  inline void loadWithMalloc() {
    addr = (char*)getFileData(chunkFile.c_str(), chunk_sz * nchunks, 0);
  }

  inline void loadWithmmap() {
    addr = (char*)getFileData(chunkFile.c_str(), chunk_sz * nchunks, 1);
  }
  inline uintE* getWithmmap(cid_t cid, uint32_t coff) {
    return (uintE*)((char*)addr + cid * chunk_sz + coff);
  }

  inline void loadWithDIO() {
    chunk_fd = open(chunkFile.c_str(), O_RDONLY | O_DIRECT | O_NOATIME);
    if(chunk_fd == -1) {
      fprintf(stdout,"Wrong open %s\n", chunkFile.c_str());
      perror("open");
      exit(-1);
    }
    cout << "Open file " << chunkFile << ", chunk_fd = " << chunk_fd << endl;

    addr = 0;
    if (chunk_sz >= HUGE_PAGE_SIZE) {
      addr = (char*)mmap(NULL, chunk_sz*nbuff, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_HUGE_2MB, 0, 0);
      if (addr == MAP_FAILED) {	
        perror("mmap");
        addr = 0;
      }
    }
    if (addr == 0) {
      addr = (char*)calloc(nbuff, chunk_sz);
    }

    int ret;
    /* allocate 1 KB along a 256-byte boundary */
    // ret = posix_memalign (&buf, 256, 1024);
    ret = posix_memalign(&addr, chunk_sz, chunk_sz*nchunks);
    if (ret) {
      fprintf (stderr, "posix_memalign: %s\n", strerror(ret));
      exit(-1);
    }
    memset(addr, 0, chunk_sz*nchunks);
  }

  inline uintE* getWithDIO(cid_t cid, uint32_t coff) {
    // choose a position to load chunk (FIFO)
    // todo
    // load chunk using preada
    // todo
    return 0;
  }

  inline long getChunkSize() { return chunk_sz; }
  inline long getChunkNum() { return nchunks; }
  inline long getLevel() { return level; }

  inline void readGraph() {
    loadWithmmap();
  }

  inline uintE* getChunkNeighbors(cid_t cid, uint32_t coff) {
    return getWithmmap(cid, coff);
  }
};

class SuperVertexManager {
private:
  std::string svFile;
  long sv_size = 0;
  char* addr = 0;

public:
  SuperVertexManager() {}
  SuperVertexManager(std::string iFile, long s) : svFile(iFile), sv_size(s) {}

  ~SuperVertexManager() {
    if (addr != 0) free(addr);
  }

  inline void loadWithMalloc() {
    addr = getFileData(svFile.c_str(), sv_size, 0);
  }

  inline void loadWithmmap() {
    addr = getFileData(svFile.c_str(), sv_size, 1);
  }

  inline void readGraph() {
    loadWithmmap();
  }

  inline long getSVSize() { return sv_size; }
  inline char* getSVAddr() { return addr; }
};

class TriLevelManager {
private:
  TriLevelReader* reader;
  // level chunk
  ChunkManager** chunkManager[DIRECT_GRAPH];
  SuperVertexManager* svManager[DIRECT_GRAPH];
  // reorder list
  bool reorderListEnable = 1;
  uintE* reorderList[DIRECT_GRAPH];

public:
  TriLevelManager() {
    reader = new TriLevelReader();
  }

  ~TriLevelManager() {
    delete reader;
    for (long i = 0; i < reader->level; i++) {
      delete chunkManager[0][i];
      delete chunkManager[1][i];
    }
    free(chunkManager[0]);
    free(chunkManager[1]);
    delete svManager[0];
    delete svManager[1];
    if (reorderListEnable) {
      uintE* addr = reorderList[0] > reorderList[1] ? reorderList[1] : reorderList[0];
      munmap(reorderList[0], reader->n * sizeof(uintE) * 2);
    }
  }

  void init() {
    // read OutGraph
    chunkManager[0] = (ChunkManager**)calloc(reader->level, sizeof(ChunkManager*));
    for (long i = 0; i < reader->level; i++) {
      std::string chunkFile = reader->chunkFile + std::to_string(i);
      std::cout << "chunkFile = " << chunkFile << std::endl;
      chunkManager[0][i] = new ChunkManager(chunkFile, reader->chunk_sz[i], reader->nchunks[i], i);
      chunkManager[0][i]->readGraph();
    }
    
    svManager[0] = new SuperVertexManager(reader->svFile, reader->sv_size);
    svManager[0]->readGraph();

    // read InGraph
    chunkManager[1] = (ChunkManager**)calloc(reader->level, sizeof(ChunkManager*));
    for (long i = 0; i < reader->level; i++) {
      std::string chunkFile = reader->rchunkFile + std::to_string(i);
      std::cout << "chunkFile = " << chunkFile << std::endl;
      chunkManager[1][i] = new ChunkManager(chunkFile, reader->rchunk_sz[i], reader->rnchunks[i], i);
      chunkManager[1][i]->readGraph();
    }

    svManager[1] = new SuperVertexManager(reader->rsvFile, reader->rsv_size);
    svManager[1]->readGraph();

    // read reorder list
    if (reorderListEnable) {
      uintE* addr = (uintE*)getFileData(reader->reorderListFile.c_str(), reader->n * sizeof(uintE) * 2, 0, 1);
      reorderList[0] = addr;
      reorderList[1] = addr + reader->n;
      #ifdef DEBUG_EN
      std::string item = "Reorder MetaData";
      size_t size = reader->n * sizeof(uintE) * 2;
      memory_profiler.memory_usage[item] = size;
      std::cout << "Allocate reorderList size: " << B2GB(size) << "GB" << std::endl;
      #endif
    }
  }

  inline TriLevelReader* getReader() { return reader; }
  inline char* getSVAddr(bool inGraph) { return svManager[inGraph]->getSVAddr(); }

  inline uintE* getChunkNeighbors(cid_t cid, uint32_t coff, long level, uint32_t d, bool inGraph=0) {
    return chunkManager[inGraph][level]->getChunkNeighbors(cid, coff);
  }

  inline void setReorderListEnable(bool enable) { reorderListEnable = enable; }
  inline bool getReorderListEnable() { return reorderListEnable; }
  inline uintE* getReorderList(bool inGraph) { return reorderList[inGraph]; }
  inline uintE getReorderListElement(bool inGraph, uintE i) { return reorderList[inGraph][i]; }
  
  inline void transpose() {
    ChunkManager** tmp = chunkManager[0];
    chunkManager[0] = chunkManager[1];
    chunkManager[1] = tmp;
    SuperVertexManager* tmp1 = svManager[0];
    svManager[0] = svManager[1];
    svManager[1] = tmp1;
    if (reorderListEnable) {
      uintE* tmp2 = reorderList[0];
      reorderList[0] = reorderList[1];
      reorderList[1] = tmp2;
    }
  }
};