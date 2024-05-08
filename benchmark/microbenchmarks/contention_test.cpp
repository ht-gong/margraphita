#include <math.h>
#include <stdio.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <deque>
#include <fstream>
#include <iostream>
#include <queue>
#include <set>
#include <vector>

#include "adj_list.h"
#include "benchmark_definitions.h"
#include "command_line.h"
#include "common_util.h"
#include "csv_log.h"
#include "edgekey.h"
#include "graph_engine.h"
#include "graph_exception.h"
#include "platform_atomics.h"
#include "pvector.h"
#include "standard_graph.h"
#include "times.h"

#define BENCHMARK_SIZE 64000  // multiple of 16

std::vector<std::pair<uint32_t, uint32_t>> to_delete;
std::vector<std::pair<uint32_t, uint32_t>> to_insert;
const int THREAD_NUM = 1;

void setup(GraphEngine* graphEngine)
{
    GraphBase* graph = graphEngine->create_graph_handle();


    uint64_t edge_count = graph->get_num_edges();

    uint64_t node_count = graph->get_num_nodes();

    EdgeCursor* edge_cursor = graph->get_edge_iter();
    edge e;
    edge_cursor->next(&e);
    int itered = 0;
    while (to_delete.size() < BENCHMARK_SIZE)
    {
        if (std::rand() % edge_count < BENCHMARK_SIZE ||
            edge_count - itered < BENCHMARK_SIZE - to_delete.size())
        {
            to_delete.push_back(
                std::pair<uint32_t, uint32_t>(e.src_id, e.dst_id));
            cout<<e.src_id <<" " << e.dst_id<<"\n";


            std::pair<uint32_t, uint32_t> gen = {e.src_id,
                                                std::rand() % UINT32_MAX};
            while(graph->has_edge(gen.first, gen.second)) {
                gen = {e.src_id,  std::rand() % node_count};
            }
            to_insert.push_back({gen});
           
        }
        itered++;
        edge_cursor->next(&e);
    }

    graph->close(true);
}

int main(int argc, char* argv[])
{
    std::cout << "Running Iteration" << std::endl;
    CmdLineApp iter_cli(argc, argv);
    if (!iter_cli.parse_args())
    {
        return -1;
    }

    cmdline_opts opts = iter_cli.get_parsed_opts();
    opts.stat_log += "/" + opts.db_name;

   
    Times t;
    t.start();
    GraphEngine graphEngine(THREAD_NUM, opts);
    setup(&graphEngine);
    t.stop();

    std::cout << "Graph loaded in " << t.t_micros() << std::endl;
    t.start();

    int rollback_count = 0;

#pragma omp parallel for num_threads(THREAD_NUM) reduction(+ : rollback_count)
    for (int i = 0; i < THREAD_NUM; i++)
    {
        GraphBase* graph = graphEngine.create_graph_handle();
        int per_thread = BENCHMARK_SIZE / THREAD_NUM;
        for (int j = i * per_thread; j < (i + 1) * per_thread; j++)
        {
            while (graph->delete_edge(to_delete[j].first,
                                      to_delete[j].second) == WT_ROLLBACK)
            {
                rollback_count++;
            }
            while (graph->add_edge(
                       edge{to_insert[j].first, to_insert[j].second}, false) ==
                   WT_ROLLBACK)
            {
                rollback_count++;
            }
        }
        graph->close(true);
    }
    t.stop();
    std::cout << "Insertion-deletion completed in : " << t.t_micros()
              << std::endl;
    std::cout << "Rollback count:" << rollback_count << std::endl;

    return 0;
}