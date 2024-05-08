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
#include "omp.h"
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

#define BENCHMARK_SIZE 6400  // multiple of 16

const int THREAD_NUM = omp_get_max_threads();


std::vector<node> read_nodes() {
    std::ifstream file("node.txt");
    vector<node> n;
    
    // Check if the file is opened successfully
    if (!file.is_open()) {
        std::cerr << "Error opening file." << std::endl;
    }

    std::string line;
    // Read lines from the file until the end
    while (std::getline(file, line)) {
        // Process each line here
        n.push_back(node{.id = static_cast<uint32_t>(std::stoul(line))});
    }

    // Close the file
    file.close();
    return n;
}

std::vector<edge> read_edges() {
    std::ifstream file("edge.txt");
    vector<edge> n;
    // Check if the file is opened successfully
    if (!file.is_open()) {
        std::cerr << "Error opening file." << std::endl;
    }

    std::string line;
    // Read lines from the file until the end
    while (std::getline(file, line)) {
        // Process each line here
        std::stringstream ss(line);
        edge_id_t src, dst;
        ss >> src;
        ss >> dst;
        n.push_back(edge{.src_id = src, .dst_id = dst});
    }

    // Close the file
    file.close();
    return n;
}

void two_reps()
{
    graph_opts opts_acc = graph_opts{.create_new = true, .read_optimize=true, .is_directed=true, .is_weighted=false, .db_name="test_acc", .db_dir="./test_acc",
    .type = GraphType::EKey};
    opts_acc.stat_log += "/" + opts_acc.db_name;

    graph_opts opts_loan = graph_opts{.create_new = true, .read_optimize=true, .is_directed=true, .is_weighted=false, .db_name="test_loan", .db_dir="./test_loan",
    .type = GraphType::EKey};
    opts_loan.stat_log += "/" + opts_loan.db_name;

    Times t;
    t.start();
    GraphEngine graphEngine_acc(THREAD_NUM, opts_acc);
    GraphEngine graphEngine_loan(THREAD_NUM, opts_loan);

    GraphBase* acc = graphEngine_acc.create_graph_handle();
    GraphBase* loan = graphEngine_loan.create_graph_handle();
    std::vector<node> n = read_nodes();
    std::vector<edge> e = read_edges();

    for(auto i : n) {
        if(i.id > 2000000000)
            loan->add_node(i);
        else 
            acc->add_node(i);
    }

     for(auto i : e) {
        acc->add_edge(i, false);
    }
    
    t.stop();
    std::cout << "Graph loaded in " << t.t_micros() << std::endl;


    
    for(int i = 0; i < 10; i++) {
        t.start();
    adjlist nbhd;
    OutCursor* iter = acc->get_outnbd_iter();
    iter->set_key_range(key_range(0, UINT32_MAX));
    iter->next(&nbhd);
    while(nbhd.node_id != UINT32_MAX) {
        for(node_id_t id: nbhd.edgelist) {
            node tmp = loan->get_node(id);
        }

        nbhd.clear();
        iter->next(&nbhd);
    }
    t.stop();
    std::cout << "2 reps finished in " << t.t_micros() << std::endl;
    }
}


void one_query_itself()
{
    graph_opts opts_acc = graph_opts{.create_new = true, .read_optimize=true, .is_directed=true, .is_weighted=false, .db_name="test_acc", .db_dir="./test_acc",
    .type = GraphType::EKey};
    opts_acc.stat_log += "/" + opts_acc.db_name;

    Times t;
    t.start();
    GraphEngine graphEngine_acc(THREAD_NUM, opts_acc);

    GraphBase* acc = graphEngine_acc.create_graph_handle();
    std::vector<node> n = read_nodes();
    std::vector<edge> e = read_edges();

    for(auto i : n) {
        acc->add_node(i);
    }

    for(auto i : e) {
        acc->add_edge(i, false);
    }
    
    t.stop();
    std::cout << "Graph loaded in " << t.t_micros() << std::endl;

    for(int i = 0; i < 10; i++) {
    t.start();
    adjlist nbhd;
    OutCursor* iter = acc->get_outnbd_iter();
    iter->set_key_range(key_range(0, UINT32_MAX));
    iter->next(&nbhd);
    int cnt = 0;
    while(nbhd.node_id != UINT32_MAX) {
        for(node_id_t id: nbhd.edgelist) {
           cnt += id;
        }
        nbhd.clear();
        iter->next(&nbhd);
    }
    t.stop();
    std::cout << cnt << "1 reps finished in " << t.t_micros() <<  std::endl;
    }
}

void one_partition()
{
    graph_opts opts_acc = graph_opts{.create_new = true, .read_optimize=true, .is_directed=true, .is_weighted=false, .db_name="test_acc", .db_dir="./test_acc",
    .type = GraphType::EKey};
    opts_acc.stat_log += "/" + opts_acc.db_name;

    Times t;
    t.start();
    GraphEngine graphEngine_acc(THREAD_NUM, opts_acc);

    GraphBase* acc = graphEngine_acc.create_graph_handle();
    std::vector<node> n = read_nodes();
    std::vector<edge> e = read_edges();

    for(auto i : n) {
        acc->add_node(i);
    }

     for(auto i : e) {
        acc->add_edge(i, false);
    }
    
    t.stop();
    std::cout << "Graph loaded in " << t.t_micros() << std::endl;

    for(int i = 0; i < 10; i++) {
    t.start();
    adjlist nbhd;
    OutCursor* iter = acc->get_outnbd_iter();
    iter->set_key_range(key_range(0, UINT32_MAX));
    iter->next(&nbhd);
    while(nbhd.node_id != UINT32_MAX) {
        for(node_id_t id: nbhd.edgelist) {
             if(id > 2000000000)
                break;
        }
        nbhd.clear();
        iter->next(&nbhd);
    }
    t.stop();
    std::cout << "1 reps finished in " << t.t_micros() << std::endl;
    }
}


int main(int argc, char* argv[])
{
    std::cout << "Running 2 reps" << std::endl;
    two_reps();
    one_query_itself();
    one_partition();

    return 0;
}