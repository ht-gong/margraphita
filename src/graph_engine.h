#ifndef GRAPH_ENGINE
#define GRAPH_ENGINE

#include "../utils/thread_utils.h"
#include "adj_list.h"
#include "common.h"
#include "edgekey.h"
#include "graph.h"
#include "graph_exception.h"
#include "lock.h"
#include "standard_graph.h"

using namespace std;

class GraphEngine
{
   public:
    struct graph_engine_opts
    {
        int num_threads;
        graph_opts opts;
    };

    GraphEngine(graph_engine_opts engine_opts);
    ~GraphEngine();
    GraphBase* create_graph_handle();
    void close_graph();
    edge_range get_edge_range(int thread_id);
    key_range get_key_range(int thread_id);

   protected:
    WT_CONNECTION* conn = nullptr;
    GraphBase* graph_stats;
    std::vector<key_range> node_ranges;
    std::vector<edge_range> edge_ranges;
    LockSet* locks;
    int num_threads;
    graph_opts opts;

    void check_opts_valid();
    void create_new_graph();
    void open_connection();
    void close_connection();
    WT_CONNECTION* get_connection();
};

#endif