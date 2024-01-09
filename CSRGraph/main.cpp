#include "apps/graph_benchmarks.hpp"
#include "src/chunkgraph.hpp"


int main(int argc, const char ** argv)
{
    args_config(argc, argv);
    print_config();
    
    switch (JOB) {
    case 1: {
        CSRGraph *csrgraph = new CSRGraph();
        // csrgraph->import_graph();
        // csrgraph->count_degree();
        csrgraph->convert_blaze();
        test_out_neighbors(csrgraph);
        test_in_neighbors(csrgraph);
        test_graph_benchmarks(csrgraph);
        delete csrgraph;
        break;
    }
    case 2: {
        BaseChunkGraph *basechunkgraph = new BaseChunkGraph();
        basechunkgraph->convert_graph();
        basechunkgraph->save_graph();
        test_out_neighbors(basechunkgraph);
        test_in_neighbors(basechunkgraph);
        test_graph_benchmarks(basechunkgraph);
        delete basechunkgraph;
        break;
    }
    case 3: {
        InplaceGraph *inplacegraph = new InplaceGraph();
        inplacegraph->convert_graph();
        inplacegraph->save_graph();
        test_out_neighbors(inplacegraph);
        test_in_neighbors(inplacegraph);
        test_graph_benchmarks(inplacegraph);
        delete inplacegraph;
        break;
    }
    case 4: {
        ReGraph *regraph = new ReGraph();
        regraph->convert_graph();
        regraph->save_graph();
        test_out_neighbors(regraph);
        test_in_neighbors(regraph);
        test_graph_benchmarks(regraph);
        delete regraph;
        break;
    }
    case 5: {
        MAX_LEVEL = 2;
        TriLevelGraph *trigraph = new TriLevelGraph();
        trigraph->convert_graph();
        trigraph->save_graph();
        test_out_neighbors(trigraph);
        test_in_neighbors(trigraph);
        test_graph_benchmarks(trigraph);
        delete trigraph;
        break;
    }
    case 6: {
        MAX_LEVEL = 2;
        ReorderGraph *reordergraph = new ReorderGraph();
        reordergraph->convert_graph();
        reordergraph->save_graph();
        test_out_neighbors(reordergraph);
        test_in_neighbors(reordergraph);
        test_graph_benchmarks(reordergraph);
        delete reordergraph;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
        break;
    }
    case 7: {
        MiniVertexGraph *minivertexgraph = new MiniVertexGraph();
        minivertexgraph->convert_graph();
        test_out_neighbors(minivertexgraph);
        test_in_neighbors(minivertexgraph);
        test_graph_benchmarks(minivertexgraph);
        delete minivertexgraph;
        break;
    }
    case 8: {
        MAX_LEVEL = 2;
        TriLevelGraph *trigraph = new TriLevelGraph();
        trigraph->convert_graph_without_reorder();
        trigraph->save_graph();
        test_out_neighbors(trigraph);
        test_in_neighbors(trigraph);
        test_graph_benchmarks(trigraph);
        delete trigraph;
        break;
    }
    case 9: {
        MiniVertexGraph *minivertexgraph = new MiniVertexGraph();
        minivertexgraph->convert_graph2();
        test_out_neighbors(minivertexgraph);
        test_in_neighbors(minivertexgraph);
        test_graph_benchmarks(minivertexgraph);
        delete minivertexgraph;
        break;
    }
    case 10: {
        MAX_LEVEL = 2;
        ReorderIDGraph *reorderidgraph = new ReorderIDGraph();
        reorderidgraph->convert_graph();
        reorderidgraph->save_graph();
        test_out_neighbors(reorderidgraph);
        test_in_neighbors(reorderidgraph);
        test_graph_benchmarks(reorderidgraph);
        delete reorderidgraph;
        break;
    }
    case 11: {
        MAX_LEVEL = 2;
        ReorderIDGraph *reorderidgraph = new ReorderIDGraph();
        reorderidgraph->convert_graph_in();
        reorderidgraph->save_graph();
        test_out_neighbors(reorderidgraph);
        test_in_neighbors(reorderidgraph);
        test_graph_benchmarks(reorderidgraph);
        delete reorderidgraph;
        break;
    }
    
    default:
        break;
    }

    return 0;
}
