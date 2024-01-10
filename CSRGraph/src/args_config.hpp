#pragma once

#include "args.hpp"

using std::cout;
using std::endl;

bool HELP = 0;

void print_usage() {
    std::string help = "./exe options.\n";
    help += " -h: This message.\n";

    help += " -f: Input dataset file.\n";
    help += " -v: Vertex count.\n";
    help += " -e: Edge count need for import. Default: 0 for importing all edges in the dataset file.\n";

    help += " -q: The number of executions for each graph query algorithm.\n";

    help += " -t: Thread count for buffering and flushing. Default: 16.\n"; //Cores in your system - 1.\n";
    cout << help << endl;
}

void args_config(int argc, const char ** argv) {
    set_argc(argc, argv);
    /* ---------------------------------------------------------------------- */
    // print_usage
    HELP = get_option_int("-h", 0); // help for print_usage

    /* ---------------------------------------------------------------------- */
    // Basic arguments for graph info
    filepath = get_option_string("-f", "/data1/wr/datasets/Friendster/all/bin");  // Input dataset file.
    PREFIX = get_option_string("--prefix", "out.friendster"); // Path of pmem1 of NUMA node1.
    nverts = get_option_int("-v", 0); // Vertex count. //TT:61588415, FS:68349467, UK:105153953, K28:268435456, YW:1413511394, K29:536870912, K30:1073741824
    nedges = get_option_int("-e", 0); // Edge count need for import. Default: 0 for importing all edges in the dataset file.
    /* ---------------------------------------------------------------------- */
    // Basic arguments for system info
    JOB = get_option_int("-j", 0); // Job type.
    THD_COUNT = get_option_int("-t", 1); // Thread count for buffering and flushing. Default: 16.
    QUERY_THD_COUNT = get_option_int("-qt", 48); // Thread count for query benchmark. Default: 16.
    SSDPATH = get_option_string("--ssd", "/mnt/nvme1"); // Path of ssd
    /* ---------------------------------------------------------------------- */
    // Basic arguments for graph query benchmark
    QUERY = get_option_int("-q", 0); // The number of executions for each graph query algorithm.
    source = get_option_int("--source", -1); // The root vertex for graph query benchmark.
    reorder_level = get_option_int("--reorder_level", 0); // The level of reordering.
    uint32_t global_threshold_int = get_option_int("--global_threshold", 0); // The global threshold for reordering.
    global_threshold = (double)global_threshold_int / 100.0;
    /* ---------------------------------------------------------------------- */
    // Basic arguments for chunk allocator
    SBLK_POOL_SIZE = get_option_long("--sblk_pool_size", 128) * GB; // Size of ssd pool for chunk allocator. Default: 128GB.
    sblk_name = get_option_string("--sblk_name", "sblk"); // Name of ssd pool for chunk allocator. Default: sblk.
    MAX_LEVEL = get_option_int("--max_level", 1); // Max level of chunk allocator. Default: 1.
    MEM_BULK_SIZE = get_option_int("--mem_bulk_size", 32) * MB; // Size of memory bulk for vertex allocator. Default: 32MB.
    /* ---------------------------------------------------------------------- */
    // Basic arguments for result
    ofs.open("result.txt", std::ofstream::out | std::ofstream::app);

    omp_set_num_threads(THD_COUNT);
}

void print_config(){
    /* ---------------------------------------------------------------------- */
    // print_usage
    if(HELP == 1){
        print_usage();
        exit(0);
    }

    /* ---------------------------------------------------------------------- */
    // Basic arguments for graph info
    cout<< "Print config information:" << endl;
    cout<< "  Graph info: " << endl;
    cout<< "\t - filepath = " << filepath << endl;
    cout<< "\t - PREFIX = " << PREFIX << endl;
    cout<< "\t - nverts = " << nverts << endl;
    cout<< "\t - nedges = " << nedges << " (0 for processing all edges in the filepath) " << endl;
    /* ---------------------------------------------------------------------- */
    // Basic arguments for system info
    cout<< "  System info: " << endl;
    cout<< "\t - JOB = " << JOB << endl;
    cout<< "\t - THD_COUNT = " << THD_COUNT << endl;
    cout<< "\t - QUERY_THD_COUNT = " << QUERY_THD_COUNT << endl;
    cout<< "\t - SSDPATH = " << SSDPATH << endl;
    /* ---------------------------------------------------------------------- */
    // Basic arguments for graph query benchmark
    cout<< "  Graph query benchmark: " << endl;
    cout<< "\t - QUERY = " << QUERY << endl;
    cout<< "\t - source = " << source << endl;
    /* ---------------------------------------------------------------------- */
    // Basic arguments for chunk allocator
    cout<< "  Chunk allocator: " << endl;
    cout<< "\t - SBLK_POOL_SIZE = " << SBLK_POOL_SIZE << endl;
    cout<< "\t - sblk_name = " << sblk_name << endl;
    cout<< "\t - MAX_LEVEL = " << MAX_LEVEL << endl;
    cout<< "\t - MEM_BULK_SIZE = " << MEM_BULK_SIZE << endl;
    cout<< endl;
    /* ---------------------------------------------------------------------- */
    // Basic arguments for result
    ofs << "[Job-" << JOB << "]:" << PREFIX << endl;
}
