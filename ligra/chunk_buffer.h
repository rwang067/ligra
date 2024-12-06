#pragma once
#include <spdk/env.h>
#include <spdk/ioat.h>
#include <spdk/nvme.h>
#include <spdk/stdinc.h>
#include <spdk/string.h>

#include "parallel.h"
#include "monitor.h"
#include "utils.h"

using namespace std;

#define MAX_IO_SIZE 64 * 1024 * 1024 // 64MB
// #define MAX_IO_SIZE 4 * 1024 // 64MB
const size_t MAX_BUFFER_POOL_SIZE = (size_t)2 * 1024 * 1024 * 1024; // 16GB

static TAILQ_HEAD(, ctrlr_entry) g_controllers	= TAILQ_HEAD_INITIALIZER(g_controllers);  // nvme controller
static TAILQ_HEAD(, ns_entry) g_namespaces	= TAILQ_HEAD_INITIALIZER(g_namespaces);       // nvme namespace
static struct spdk_nvme_qpair ** g_qpair = NULL;
static uint32_t sector_size = 0;

struct ctrlr_entry {
	struct spdk_nvme_ctrlr			*ctrlr;
	TAILQ_ENTRY(ctrlr_entry)		link;
	char					name[1024];
};

struct ns_entry {
	struct {
		struct spdk_nvme_ctrlr		*ctrlr;
		struct spdk_nvme_ns		*ns;
	} nvme;

	TAILQ_ENTRY(ns_entry)			link;
	char					name[1024];
};


struct chunkgraph_sequence {
  struct ns_entry *ns_entry;
  void* buf;
  int is_completed;
  bool using_cmb_io;
  struct spdk_nvme_qpair *qpair = NULL;
};

static bool probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid, struct spdk_nvme_ctrlr_opts *opts) {
  printf("Attaching to %s\n", trid->traddr);
  return true;
}

static void register_ns(struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_ns *ns) {
  if (!spdk_nvme_ns_is_active(ns))
    return;

  struct ns_entry *entry = static_cast<ns_entry*>(calloc(1, sizeof(struct ns_entry)));
  if (entry == NULL) {
    perror("ns_entry malloc");
    exit(1);
  }

  entry->nvme.ctrlr = ctrlr;
  entry->nvme.ns = ns;
  TAILQ_INSERT_TAIL(&g_namespaces, entry, link);
}

static void attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
    struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts) {
  uint32_t nsid;
  struct spdk_nvme_ns *ns;
  struct ctrlr_entry *entry;
  const struct spdk_nvme_ctrlr_data* cdata;

  entry = static_cast<ctrlr_entry*>(calloc(1, sizeof(struct ctrlr_entry)));
  if (entry == NULL) {
    perror("ctrlr_entry malloc");
    exit(1);
  }

  printf("Attached to %s\n", trid->traddr);

  cdata = spdk_nvme_ctrlr_get_data(ctrlr);
  snprintf(entry->name, sizeof(entry->name), "%-20.20s (%-20.20s)", cdata->mn, cdata->sn);

  entry->ctrlr = ctrlr;
  TAILQ_INSERT_TAIL(&g_controllers, entry, link);

  for (nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr); nsid != 0;
      nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, nsid)) {
    ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
    if (ns == NULL) {
      continue;
    }
    register_ns(ctrlr, ns);
  }
}

static void cleanup(void) {
  struct ns_entry *entry, *tmp_entry;
  struct ctrlr_entry *ctrlr_entry, *tmp_ctrlr_entry;
	struct spdk_nvme_detach_ctx *detach_ctx = NULL;

  if (g_qpair) {
      int max_core = getWorkers();
      for (int i = 0; i < max_core; i++) {
        spdk_nvme_ctrlr_free_io_qpair(g_qpair[i]);
      }
      free(g_qpair);
    }

  TAILQ_FOREACH_SAFE(entry, &g_namespaces, link, tmp_entry) {
    TAILQ_REMOVE(&g_namespaces, entry, link);
    free(entry);
  }

  TAILQ_FOREACH_SAFE(ctrlr_entry, &g_controllers, link, tmp_ctrlr_entry) {
    TAILQ_REMOVE(&g_controllers, ctrlr_entry, link);
    spdk_nvme_detach_async(ctrlr_entry->ctrlr, &detach_ctx);
    free(ctrlr_entry);
  }

  if (detach_ctx) {
		spdk_nvme_detach_poll(detach_ctx);
	}

  // free memory used by SPDK
  spdk_env_fini();
}

void write_complete(void *arg, const struct spdk_nvme_cpl *completion) {
  struct chunkgraph_sequence *sequence = (struct chunkgraph_sequence *)arg;
  struct ns_entry *ns_entry = sequence->ns_entry;
  sequence->is_completed = 1;
  if (spdk_nvme_cpl_is_error(completion)) {
    spdk_nvme_qpair_print_completion(sequence->qpair, (struct spdk_nvme_cpl *)completion);
    fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
    fprintf(stderr, "Write I/O failed, aborting run\n");
		sequence->is_completed = 2;
		exit(1);
  }
  // printf("Write complete\n");
}

void read_complete(void *arg, const struct spdk_nvme_cpl *completion) {
  struct chunkgraph_sequence *sequence = (struct chunkgraph_sequence *)arg;
  sequence->is_completed = 1;
  if (spdk_nvme_cpl_is_error(completion)) {
		spdk_nvme_qpair_print_completion(sequence->qpair, (struct spdk_nvme_cpl *)completion);
		fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
		fprintf(stderr, "Read I/O failed, aborting run\n");
		sequence->is_completed = 2;
		exit(1);
	}
  // printf("Read complete\n");
}



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
    if (size == 0) return NULL;
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
    if (isProfile) {
      preada(fd, addr, size, 0);
    }
  }
  return addr;
}

char* getFileDataHuge(const char* filename, size_t size = 0, bool isProfile = 0) {
  char* addr = 0;
  if (size == 0) return NULL;
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
  addr = (char*)mmap(NULL, size, PROT_READ|PROT_WRITE,MAP_SHARED|MAP_HUGETLB|MAP_HUGE_2MB, fd, 0);
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
    #define HUGE_PAGE_SIZE 2097152 // 2MB
    if (chunk_sz >= HUGE_PAGE_SIZE) return (uintE*)((char*)addr + cid * chunk_sz + coff);
    else return (uintE*)((char*)addr + cid * chunk_sz + coff);
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
  bool onlyout = 0;
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

    if (!onlyout) {
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
    }
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
  inline void setOnlyOut(bool onlyout) { this->onlyout = onlyout; }
  inline bool getOnlyOut() { return onlyout; }
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

class BufferManager {
public:
  BufferManager() {}
  ~BufferManager() {}

  virtual long getNextBuffer(cid_t chunk_id = 0) = 0;
  virtual cid_t getBufferID(cid_t chunk_id) = 0;
  virtual cid_t getChunkID(cid_t buffer_id) = 0;
  virtual void setBufferID(cid_t chunk_id, cid_t buffer_id) = 0;
  virtual void setChunkID(cid_t buffer_id, cid_t chunk_id) = 0;
  virtual bool isHit(cid_t chunk_id) = 0;
};

class FIFOBufferManager : public BufferManager {
public:
  FIFOBufferManager() {}
  FIFOBufferManager(long s, long n, long l) : chunk_sz(s), nchunks(n), num_buf(l) {
    chunk_id_map = new cid_t[nchunks];
    buffer_id_map = new cid_t[num_buf];
    memset(chunk_id_map, -1, nchunks * sizeof(cid_t));
    memset(buffer_id_map, -1, num_buf * sizeof(cid_t));
  }
  ~FIFOBufferManager() {}

  // get current chunk id and update the pointer, get should be atomic
  inline long getNextBuffer(cid_t chunk_id = 0) {
    long res = curr % num_buf;
    if (buffer_id_map[res] != -1) {
      chunk_id_map[buffer_id_map[res]] = -1;
    }
    buffer_id_map[res] = chunk_id;
    chunk_id_map[chunk_id] = res;
    __sync_fetch_and_add(&curr, 1);
    return res;
  }

  inline cid_t getBufferID(cid_t chunk_id) {
    return chunk_id_map[chunk_id];
  }

  inline cid_t getChunkID(cid_t buffer_id) {
    return buffer_id_map[buffer_id];
  }

  inline void setBufferID(cid_t chunk_id, cid_t buffer_id) {
    chunk_id_map[chunk_id] = buffer_id;
  }

  inline void setChunkID(cid_t buffer_id, cid_t chunk_id) {
    buffer_id_map[buffer_id] = chunk_id;
  }

  inline bool isHit(cid_t chunk_id) {
    return chunk_id_map[chunk_id] != -1;
  }

private:
  long chunk_sz = 0;
  long nchunks = 0;
  long num_buf = 0;
  long curr = 0;

  cid_t *chunk_id_map = 0;  // chunk_id_map[chunk_id] = buffer_id, query buffer_id by chunk_id
  cid_t *buffer_id_map = 0; // buffer_id_map[buffer_id] = chunk_id, query chunk_id by buffer_id
};

class MultiFIFOBufferManager : public BufferManager {
public:
  struct ThreadManager {
    long range_size = 0;
    long range_begin = 0;
    long curr = 0;  
    cid_t *chunk_id_map = 0;  // chunk_id_map[chunk_id] = buffer_id, query buffer_id by chunk_id
    cid_t *buffer_id_map = 0; // buffer_id_map[buffer_id] = chunk_id, query chunk_id by buffer_id

    long getNextBuffer(cid_t chunk_id = 0) {
      long res = curr % range_size;
      if (buffer_id_map[res] != -1) {
        chunk_id_map[buffer_id_map[res]] = -1;
      }
      buffer_id_map[res] = chunk_id;
      chunk_id_map[chunk_id] = res;
      curr += 1;
      return res + range_begin;
    }

    cid_t getBufferID(cid_t chunk_id) {
      return chunk_id_map[chunk_id];
    }

    cid_t getChunkID(cid_t buffer_id) {
      return buffer_id_map[buffer_id];
    }

    void setBufferID(cid_t chunk_id, cid_t buffer_id) {
      chunk_id_map[chunk_id] = buffer_id;
    }

    void setChunkID(cid_t buffer_id, cid_t chunk_id) {
      buffer_id_map[buffer_id] = chunk_id;
    }

    bool isHit(cid_t chunk_id) {
      return chunk_id_map[chunk_id] != -1;
    }
  };

  MultiFIFOBufferManager() {}
  MultiFIFOBufferManager(long s, long n, long l) : chunk_sz(s), nchunks(n), num_buf(l) {
    long max_range = getWorkers();

    threadManager = new ThreadManager[max_range];
    for (int i = 0; i < max_range; i++) {
      threadManager[i].range_size = num_buf / max_range;
      threadManager[i].range_begin = i * threadManager[i].range_size;
      threadManager[i].curr = 0;
      threadManager[i].chunk_id_map = new cid_t[nchunks];
      threadManager[i].buffer_id_map = new cid_t[threadManager[i].range_size];
      memset(threadManager[i].chunk_id_map, -1, nchunks * sizeof(cid_t));
      memset(threadManager[i].buffer_id_map, -1, threadManager[i].range_size * sizeof(cid_t));
    }
  }
  ~MultiFIFOBufferManager() {}
  
  // get current chunk id and update the pointer, get should be atomic
  inline long getNextBuffer(cid_t chunk_id = 0) {
    return threadManager[getWorkersID()].getNextBuffer(chunk_id);
  }

  inline cid_t getBufferID(cid_t chunk_id) {
    return threadManager[getWorkersID()].getBufferID(chunk_id);
  }

  inline cid_t getChunkID(cid_t buffer_id) {
    return threadManager[getWorkersID()].getChunkID(buffer_id);
  }

  inline void setBufferID(cid_t chunk_id, cid_t buffer_id) {
    threadManager[getWorkersID()].setBufferID(chunk_id, buffer_id);
  }

  inline void setChunkID(cid_t buffer_id, cid_t chunk_id) {
    threadManager[getWorkersID()].setChunkID(buffer_id, chunk_id);
  }

  inline bool isHit(cid_t chunk_id) {
    return threadManager[getWorkersID()].isHit(chunk_id);
  }

private:
  long chunk_sz = 0;
  long nchunks = 0;
  long num_buf = 0;

  ThreadManager *threadManager = 0;
};

class MultiChunkFIFOBufferManager : public BufferManager {
public:
  struct ThreadManager {
    long range_size = 0;
    long range_begin = 0;
    long max_range = 0;
    long curr = 0;  
    cid_t *chunk_id_map = 0;  // chunk_id_map[chunk_id] = buffer_id, query buffer_id by chunk_id
    cid_t *buffer_id_map = 0; // buffer_id_map[buffer_id] = chunk_id, query chunk_id by buffer_id

    long getNextBuffer(cid_t chunk_id = 0) {
      long map_id = chunk_id / max_range;
      long res = curr % range_size;
      if (buffer_id_map[res] != -1) {
        chunk_id_map[buffer_id_map[res] / max_range] = -1;
      }
      buffer_id_map[res] = chunk_id;
      chunk_id_map[map_id] = res;
      __sync_fetch_and_add(&curr, 1);
      return res + range_begin;
    }

    cid_t getBufferID(cid_t chunk_id) {
      long map_id = chunk_id / max_range;
      return chunk_id_map[map_id];
    }

    cid_t getChunkID(cid_t buffer_id) {
      return buffer_id_map[buffer_id];
    }

    void setBufferID(cid_t chunk_id, cid_t buffer_id) {
      long map_id = chunk_id / max_range;
      chunk_id_map[map_id] = buffer_id;
    }

    void setChunkID(cid_t buffer_id, cid_t chunk_id) {
      buffer_id_map[buffer_id] = chunk_id;
    }

    bool isHit(cid_t chunk_id) {
      long map_id = chunk_id / max_range;
      return chunk_id_map[map_id] != -1;
    }
  };

  MultiChunkFIFOBufferManager() {}

  MultiChunkFIFOBufferManager(long s, long n, long l) : chunk_sz(s), nchunks(n), num_buf(l) {
    max_range = getWorkers();

    threadManager = new ThreadManager[max_range];
    for (int i = 0; i < max_range; i++) {
      threadManager[i].range_size = num_buf / max_range;
      threadManager[i].range_begin = i * threadManager[i].range_size;
      threadManager[i].max_range = max_range;
      threadManager[i].curr = 0;
      long map_size = (nchunks + max_range - 1) / max_range;
      threadManager[i].chunk_id_map = new cid_t[map_size];
      threadManager[i].buffer_id_map = new cid_t[threadManager[i].range_size];
      memset(threadManager[i].chunk_id_map, -1, map_size * sizeof(cid_t));
      memset(threadManager[i].buffer_id_map, -1, threadManager[i].range_size * sizeof(cid_t));
    }
  }

  ~MultiChunkFIFOBufferManager() {}

  inline long getRangeID(cid_t chunk_id) {
    return chunk_id % max_range;
  }

  // get current chunk id and update the pointer, get should be atomic
  inline long getNextBuffer(cid_t chunk_id = 0) {
    return threadManager[getRangeID(chunk_id)].getNextBuffer(chunk_id);
  }

  inline cid_t getBufferID(cid_t chunk_id) {
    return threadManager[getRangeID(chunk_id)].getBufferID(chunk_id);
  }

  inline cid_t getChunkID(cid_t buffer_id) {
    long range_id = 0;
    return threadManager[range_id].getChunkID(buffer_id);
  }

  inline void setBufferID(cid_t chunk_id, cid_t buffer_id) {
    threadManager[getRangeID(chunk_id)].setBufferID(chunk_id, buffer_id);
  }

  inline void setChunkID(cid_t buffer_id, cid_t chunk_id) {
    threadManager[getRangeID(chunk_id)].setChunkID(buffer_id, chunk_id);
  }

  inline bool isHit(cid_t chunk_id) {
    return threadManager[getRangeID(chunk_id)].isHit(chunk_id);
  }

private:
  long chunk_sz = 0;
  long nchunks = 0;
  long num_buf = 0;

  long max_range = 0;
  ThreadManager *threadManager = 0;
};

class MultiChunkRangeBufferManager : public BufferManager {
public:
  struct ThreadManager {
    long range_size = 0;
    long range_begin = 0;
    long max_range = 0;
    long curr = 0;  
    cid_t *chunk_id_map = 0;  // chunk_id_map[chunk_id] = buffer_id, query buffer_id by chunk_id
    cid_t *buffer_id_map = 0; // buffer_id_map[buffer_id] = chunk_id, query chunk_id by buffer_id

    inline long getNextBuffer(cid_t chunk_id = 0) {
      long map_id = chunk_id / max_range;
      long res = curr % range_size;
      if (buffer_id_map[res] != -1) {
        chunk_id_map[buffer_id_map[res] / max_range] = -1;
      }
      buffer_id_map[res] = chunk_id;
      chunk_id_map[map_id] = res;
      // __sync_fetch_and_add(&curr, 1);
      curr++;
      return res + range_begin;
    }

    inline cid_t getBufferID(cid_t chunk_id) {
      long map_id = chunk_id / max_range;
      return chunk_id_map[map_id];
    }

    inline cid_t getChunkID(cid_t buffer_id) {
      return buffer_id_map[buffer_id];
    }

    inline void setBufferID(cid_t chunk_id, cid_t buffer_id) {
      long map_id = chunk_id / max_range;
      chunk_id_map[map_id] = buffer_id;
    }

    inline void setChunkID(cid_t buffer_id, cid_t chunk_id) {
      buffer_id_map[buffer_id] = chunk_id;
    }

    inline bool isHit(cid_t chunk_id) {
      long map_id = chunk_id / max_range;
      return chunk_id_map[map_id] != -1;
    }
  };

  MultiChunkRangeBufferManager() {}

  MultiChunkRangeBufferManager(long s, long n, long l) : chunk_sz(s), nchunks(n), num_buf(l) {
    max_range = getWorkers();

    threadManager = new ThreadManager[max_range];
    for (int i = 0; i < max_range; i++) {
      threadManager[i].range_size = num_buf / max_range;
      threadManager[i].range_begin = i * threadManager[i].range_size;
      threadManager[i].max_range = max_range;
      threadManager[i].curr = 0;
      long map_size = (nchunks + max_range - 1) / max_range;
      threadManager[i].chunk_id_map = new cid_t[map_size];
      threadManager[i].buffer_id_map = new cid_t[threadManager[i].range_size];
      memset(threadManager[i].chunk_id_map, -1, map_size * sizeof(cid_t));
      memset(threadManager[i].buffer_id_map, -1, threadManager[i].range_size * sizeof(cid_t));
    }
  }

  ~MultiChunkRangeBufferManager() {}

  inline long getRangeID(cid_t chunk_id) {
    long range_id = chunk_id % max_range;
    return range_id;
  }

  // get current chunk id and update the pointer, get should be atomic
  inline long getNextBuffer(cid_t chunk_id = 0) {
    return threadManager[getRangeID(chunk_id)].getNextBuffer(chunk_id);
  }

  inline cid_t getBufferID(cid_t chunk_id) {
    return threadManager[getRangeID(chunk_id)].getBufferID(chunk_id);
  }

  inline cid_t getChunkID(cid_t buffer_id) {
    long range_id = 0;
    return threadManager[range_id].getChunkID(buffer_id);
  }

  inline void setBufferID(cid_t chunk_id, cid_t buffer_id) {
    threadManager[getRangeID(chunk_id)].setBufferID(chunk_id, buffer_id);
  }

  inline void setChunkID(cid_t buffer_id, cid_t chunk_id) {
    threadManager[getRangeID(chunk_id)].setChunkID(buffer_id, chunk_id);
  }

  inline bool isHit(cid_t chunk_id) {
    int range_id = getRangeID(chunk_id);
    return threadManager[getRangeID(chunk_id)].isHit(chunk_id);
  }

private:
  long chunk_sz = 0;
  long nchunks = 0;
  long num_buf = 0;

  long max_range = 0;
  ThreadManager *threadManager = 0;
};

class SPDKChunkManager {
public:
  SPDKChunkManager() {}
  SPDKChunkManager(std::string iFile, long s, long n, long l, uint64_t lba = 0, uint64_t lba_count = 0)
                  : chunkFile(iFile), chunk_sz(s), nchunks(n), level(l), lba_begin(lba), lba_total_count(lba_count) {
    init();
  }

  ~SPDKChunkManager() {
    if (spdk_buffer != 0) {
      spdk_free(spdk_buffer);
    }
  }

  inline void init() {
    // init spdk controller and namespace
    ctrlr = TAILQ_FIRST(&g_controllers);
    if (ctrlr == NULL) {
      fprintf(stderr, "no NVMe controllers found\n");
      cleanup();
      exit(1);
    }
    ns = TAILQ_FIRST(&g_namespaces);
    if (ns == NULL) {
      fprintf(stderr, "no NVMe namespaces found\n");
      cleanup();
      exit(1);
    }

    // allocate a buffer for chunk
    long total_sz = chunk_sz * nchunks;
    long buff_sz = total_sz * 0.75;
    num_buf = buff_sz / chunk_sz;

    // allocate buffer using malloc
    spdk_buffer = spdk_zmalloc(buff_sz, chunk_sz, NULL, SPDK_ENV_NUMA_ID_ANY, SPDK_MALLOC_DMA);
    if (spdk_buffer == 0) {
      fprintf(stderr, "spdk_zmalloc failed at level %ld\n", level);
      cleanup();
      exit(1);
    }

    // create buffer manager
    bufferManager = new FIFOBufferManager(chunk_sz, nchunks, num_buf);
    // bufferManager = new MultiFIFOBufferManager(chunk_sz, nchunks, num_buf);
    // bufferManager = new MultiChunkFIFOBufferManager(chunk_sz, nchunks, num_buf);
    // bufferManager = new MultiChunkRangeBufferManager(chunk_sz, nchunks, num_buf);
  }

  inline void readGraph() {
    struct chunkgraph_sequence sequence;
    sequence.ns_entry = ns;
    sequence.is_completed = 0;

    // allocate read buffer
    size_t nbytes = (size_t)chunk_sz * nchunks, sz;
    size_t nbuf_bytes = min(nbytes, MAX_BUFFER_POOL_SIZE);

    // size_t nbytes = 5120, sz;
    sequence.using_cmb_io = 1;
    sequence.buf = spdk_nvme_ctrlr_map_cmb(ns->nvme.ctrlr, &sz);
    if (sequence.buf == NULL || sz < nbytes) {
      sequence.using_cmb_io = 0;
      sequence.buf = spdk_zmalloc(nbuf_bytes, chunk_sz, NULL, SPDK_ENV_NUMA_ID_ANY, SPDK_MALLOC_DMA);
      if (sequence.buf == NULL) {
        fprintf(stderr, "spdk_zmalloc failed\n");
        cleanup();
        exit(1);
      }
    }
    sequence.qpair = g_qpair[0];

    #ifdef DEBUG_EN
    if (sequence.using_cmb_io) {
			printf("INFO: using controller memory buffer for IO\n");
		} else {
			printf("INFO: using host memory buffer for IO\n");
		}
    #endif

    // read chunk data file to buffer
    int fd = open(chunkFile.c_str(), O_RDONLY);
    if (fd == -1) {
      fprintf(stderr, "could not open file %s\n", chunkFile.c_str());
      cleanup();
      exit(1);
    }

    size_t nbytes_left = nbytes;
    size_t nbytes_read = 0;
    while (nbytes_left > 0) { // we do read in a loop because the maximum buffer size is 16GB
      size_t cur_read = min(nbytes_left, nbuf_bytes);
      printf("Read %ld data from file.\n", cur_read);
      size_t total_read = 0;
      while (total_read < cur_read) {
        size_t nread = pread(fd, (char*)sequence.buf + total_read, cur_read - total_read, nbytes_read + total_read);
        if (nread == -1) {
          fprintf(stderr, "read failed\n");
          cleanup();
          exit(1);
        }
        total_read += nread;
      }

      // write chunk data to NVMe SSD through SPDK
      uint64_t current_lba_count = 0;
      uint64_t offset_lba_count = nbytes_read / sector_size;
      uint64_t end_lba_count = cur_read / sector_size;
      uint64_t max_lba_step = MAX_IO_SIZE / sector_size;

      while (current_lba_count < end_lba_count) {
        uint64_t step = std::min(end_lba_count - current_lba_count, max_lba_step);
        char* buf = (char*)sequence.buf + current_lba_count * sector_size;
        int rc = spdk_nvme_ns_cmd_write(ns->nvme.ns, sequence.qpair, buf,  
                          lba_begin + offset_lba_count + current_lba_count, /* LBA start */
                          step, /* number of LBAs */
                          write_complete, &sequence, 0);

        if (rc != 0) {
          fprintf(stderr, "starting write I/O failed\n");
          cout << "rc = " << rc << endl;
          cleanup();
          exit(1);
        }

        // wait for write completion. todo: submit multiple I/Os request instead of waiting for each I/O
        while (!sequence.is_completed) {
          spdk_nvme_qpair_process_completions(sequence.qpair, 0);
        }

        current_lba_count += step;
        sequence.is_completed = 0;
      }

      nbytes_left -= cur_read;
      nbytes_read += cur_read;
    }

    #ifdef DEBUG_EN
    // allocate a temporary buffer for read
    void* read_buf = spdk_zmalloc(2097152, 512, NULL, SPDK_ENV_NUMA_ID_ANY, SPDK_MALLOC_DMA);
    if (read_buf == NULL) {
      fprintf(stderr, "spdk_zmalloc failed\n");
      cleanup();
      exit(1);
    }
    int rc = spdk_nvme_ns_cmd_read(ns->nvme.ns, sequence.qpair, read_buf, 
                        lba_begin + 131072, /* LBA start */
                        1, /* number of LBAs */
                        read_complete, &sequence, 0);
    
    if (rc != 0) {
      fprintf(stderr, "starting read I/O failed\n");
      cleanup();
      exit(1);
    }

    // wait for read completion
    while (!sequence.is_completed) {
      spdk_nvme_qpair_process_completions(sequence.qpair, 0);
    }
    sequence.is_completed = 0;

    // print read data
    uint32_t* data = (uint32_t*)read_buf;
    for (int i = 0; i < 128; i++) {
      cout << data[i] << " ";
    }
    cout << endl << endl;

    // print write data
    data = (uint32_t*)((char*)sequence.buf + sector_size * (131072));
    for (int i = 0; i < 128; i++) {
      cout << data[i] << " ";
    }
    cout << endl << endl;

    spdk_free(read_buf);
    #endif

    // free
    if (sequence.using_cmb_io) spdk_nvme_ctrlr_unmap_cmb(ns->nvme.ctrlr);
    else spdk_free(sequence.buf);
  }

  inline char* readChunkData(cid_t cid) {
    struct chunkgraph_sequence sequence;
    sequence.ns_entry = ns;
    sequence.is_completed = 0;
    sequence.using_cmb_io = 0;
    long bufferID = bufferManager->getNextBuffer(cid);
    sequence.buf = (char*)spdk_buffer + bufferID * chunk_sz;
    int tid = getWorkersID();
    sequence.qpair = g_qpair[tid];

    // read chunk data from NVMe SSD through SPDK
    uint64_t lba_count = chunk_sz / sector_size;
    uint64_t lba_offset = lba_begin + cid * lba_count;
    // read chunk data from NVMe SSD through SPDK
    int rc = spdk_nvme_ns_cmd_read(ns->nvme.ns, sequence.qpair, sequence.buf, 
                      lba_offset, /* LBA start */
                      lba_count, /* number of LBAs */
                      read_complete, &sequence, 0);

    if (rc != 0) {
      fprintf(stderr, "starting read I/O failed\n");
      cleanup();
      exit(1);
    }

    // wait for read completion
    while (!sequence.is_completed) {
      spdk_nvme_qpair_process_completions(sequence.qpair, 0);
    }

    sequence.is_completed = 0;
    return (char*)sequence.buf;
  }
  
  inline void preLoadGraph() {
    for (cid_t cid = 0; cid < num_buf; cid++) {
      readChunkData(cid);
    }
  }

  inline uintE* getChunkNeighbors(uint64_t neighbors, uint32_t d) {
    uint64_t r = UNSET_CHUNK(neighbors);
    cid_t cid = r >> 32;
    cid_t coff = r & 0xFFFFFFFF;

    // int before_omp_tid = getWorkersID();
    // int before_tid = sched_getcpu();
    // int real_tid = cid % getWorkers();
    // bind_thread_to_cpu(real_tid);

    // int after_omp_tid = getWorkersID();
    // int after_tid = sched_getcpu();
    // printf("before_tid = %d, before_omp_tid = %d, real_tid = %d, after_tid = %d, after_omp_tid = %d\n", before_tid, before_omp_tid, real_tid, after_tid, after_omp_tid);

    if (bufferManager->isHit(cid)) {
      // get buffer id
      cid_t bid = bufferManager->getBufferID(cid);
      return (uintE*)((char*)spdk_buffer + bid * chunk_sz + coff);
    } else {
      char *chunk = readChunkData(cid);
      // return neighbors
      return (uintE*)(chunk + coff);
    }
  }

private:
  // chunk and level information
  std::string chunkFile;
  long chunk_sz = 0;
  long nchunks = 0;
  long level = 0;

  // manage a chunk buffer
  void* spdk_buffer = 0;
  long num_buf = 0;
  BufferManager* bufferManager = 0;

  // spdk controller and namespace  
  ctrlr_entry *ctrlr = NULL;
  ns_entry *ns = NULL;
  uint64_t lba_begin = 0;
  uint64_t lba_total_count = 0;
};

class SPDKManager {
public:
    SPDKManager() {}
    ~SPDKManager() {
      cleanup();
    }

    void init(TriLevelReader* reader) {
      init_spdk();
      init_chunk_manager(reader);
    }

    void init_spdk() {
        int rc;
        
        opts.opts_size = sizeof(opts);
        spdk_env_opts_init(&opts);
        opts.name = "chunkgraph";
        // opts.no_huge = true;
        // opts.iova_mode = "va";
        // opts.mem_size = 1024 * 7; // todo: hugepage can not be recycle

        rc = spdk_env_init(&opts);
        if (rc < 0) {
            std::cerr << "Unable to initialize SPDK env\n";
            exit(1);
        }

        rc = spdk_nvme_probe(NULL, NULL, probe_cb, attach_cb, NULL);
        if (rc != 0) {
          fprintf(stderr, "spdk_nvme_probe failed\n");
          cleanup();
          exit(1);
        }

        if (TAILQ_EMPTY(&g_controllers)) {
          fprintf(stderr, "no NVMe controllers found\n");
          cleanup();
          exit(1);
        }

        if (TAILQ_EMPTY(&g_namespaces)) {
          fprintf(stderr, "no NVMe namespaces found\n");
          cleanup();
          exit(1);
        }

        struct ns_entry *entry = TAILQ_FIRST(&g_namespaces);
        sector_size = spdk_nvme_ns_get_sector_size(entry->nvme.ns);

        // allocate qpair array, one qpair per core
        int max_core = getWorkers();
        g_qpair = (struct spdk_nvme_qpair**)calloc(max_core, sizeof(struct spdk_nvme_qpair*));
        for (int i = 0; i < max_core; i++) {
          g_qpair[i] = spdk_nvme_ctrlr_alloc_io_qpair(entry->nvme.ctrlr, NULL, 0);
          if (g_qpair[i] == NULL) {
            fprintf(stderr, "spdk_nvme_ctrlr_alloc_io_qpair failed\n");
            cleanup();
            exit(1);
          }
        }

        #ifdef DEBUG_EN
        uint64_t ns_size = spdk_nvme_ns_get_size(entry->nvme.ns);
        cout << "Init SPDKManager with namespace: " << entry->name << endl;
        cout << "sector_size = " << sector_size << endl;
        cout << "Namespace size: " << ns_size << " bytes" << endl;
        const struct spdk_nvme_ctrlr_data *ctrlr_data = spdk_nvme_ctrlr_get_data(entry->nvme.ctrlr);
        uint32_t max_write_size = 1 << ctrlr_data->mdts;
        cout << "max_write_size = " << max_write_size << endl;
        #endif
    }

    void init_chunk_manager(TriLevelReader* reader) {
        level = reader->level;
        chunkManager[0] = (SPDKChunkManager**)calloc(level, sizeof(SPDKChunkManager*));
        uint64_t lba_begin = 0;
        for (long i = 0; i < level; i++) {
            std::string chunkFile = reader->chunkFile + std::to_string(i);
            uint64_t file_size = reader->chunk_sz[i] * reader->nchunks[i];
            uint64_t lba_count = (file_size + sector_size - 1) / sector_size;
            #ifdef DEBUG_EN
            std::cout << "chunkFile = " << chunkFile << std::endl;
            std::cout << "lba_begin = " << lba_begin << ", lba_count = " << lba_count << std::endl;
            #endif
            chunkManager[0][i] = new SPDKChunkManager(chunkFile, reader->chunk_sz[i], reader->nchunks[i], i, lba_begin, lba_count);
            chunkManager[0][i]->readGraph();
            chunkManager[0][i]->preLoadGraph();
            lba_begin += lba_count;
        }

        chunkManager[1] = (SPDKChunkManager**)calloc(level, sizeof(SPDKChunkManager*));
        for (long i = 0; i < level; i++) {
            std::string chunkFile = reader->rchunkFile + std::to_string(i);
            uint64_t file_size = reader->rchunk_sz[i] * reader->rnchunks[i];
            uint64_t lba_count = (file_size + sector_size - 1) / sector_size;
            #ifdef DEBUG_EN
            std::cout << "chunkFile = " << chunkFile << std::endl;
            std::cout << "lba_begin = " << lba_begin << ", lba_count = " << lba_count << std::endl;
            #endif
            chunkManager[1][i] = new SPDKChunkManager(chunkFile, reader->rchunk_sz[i], reader->rnchunks[i], i, lba_begin, lba_count);
            chunkManager[1][i]->readGraph();
            chunkManager[1][i]->preLoadGraph();
            lba_begin += lba_count;
        }
    }

    uintE* getChunkNeighbors(uint64_t neighbors, long level, uint32_t d, bool inGraph=0) {
        return chunkManager[inGraph][level]->getChunkNeighbors(neighbors, d);
    }
private:
  // chunk manager
  SPDKChunkManager** chunkManager[DIRECT_GRAPH];
  long level = 0;

  // spdk manager
  struct spdk_env_opts opts;
};

class SPDKChunkGraphManager {
private:
  TriLevelReader* reader;
  // SPDK chunk manager
  SPDKManager* spdkChunkManager;
  // super vertex manager
  SuperVertexManager* svManager[DIRECT_GRAPH];

  // reorder list
  bool reorderListEnable = 1;
  uintE* reorderList[DIRECT_GRAPH];

public:
  SPDKChunkGraphManager() {
    reader = new TriLevelReader();
  }

  ~SPDKChunkGraphManager() {
    delete reader;

    delete svManager[0];
    delete svManager[1];
    delete spdkChunkManager;
    if (reorderListEnable) {
      munmap(reorderList, reader->n * sizeof(uintE) * 2);
    }
  }

  void init() {
    // super vertex
    svManager[0] = new SuperVertexManager(reader->svFile, reader->sv_size);
    svManager[0]->readGraph();

    svManager[1] = new SuperVertexManager(reader->svFile, reader->sv_size);
    svManager[1]->readGraph();

    spdkChunkManager = new SPDKManager();
    spdkChunkManager->init(reader);
  }

  inline TriLevelReader* getReader() { return reader; }
  inline char* getSVAddr(bool inGraph) { return svManager[inGraph]->getSVAddr(); }

  inline uintE* getChunkNeighbors(uint64_t neighbors, long level, uint32_t d, bool inGraph=0) {
    return spdkChunkManager->getChunkNeighbors(neighbors, level, d, inGraph);
  }

  inline void setReorderListEnable(bool enable) { reorderListEnable = enable; }
  inline bool getReorderListEnable() { return reorderListEnable; }

  inline uintE* getReorderList(bool inGraph) { return reorderList[inGraph]; }
  inline uintE getReorderListElement(bool inGraph, uintE i) { return reorderList[inGraph][i]; }
  
  inline void transpose() {
    swap(svManager[0], svManager[1]);
  }
};