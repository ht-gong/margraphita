#include <chrono>
#include <iostream>
#include <vector>

#include "benchmark_definitions.h"
#include "bitmap.h"
#include "command_line.h"
#include "csv_log.h"
#include "graph_engine.h"
#include "omp.h"
#include "platform_atomics.h"
#include "pvector.h"
#include "sliding_queue.h"
#include "times.h"

typedef float ScoreT;
typedef double CountT;

const int THREAD_NUM = omp_get_max_threads();
std::vector<node_id_t> workloads = {
    3858241, 3858242, 3858243, 3858244, 3858245, 3858246, 3858247, 3858248,
    3858249, 3858250, 3858251, 3858252, 3858253, 3858254, 3858255, 3858256,
    3858257, 3858258, 3858259, 3858260, 3858261, 3858262, 3858263, 3858264,
    3858265, 3858266, 3858267, 3858268, 3858269, 3858270, 3858271, 3858272,
    3858273, 3858274, 3858275, 3858276, 3858277, 3858278, 3858279, 3858280,
    3858281, 3858282, 3858283, 3858284, 3858285, 3858286, 3858287, 3858288,
    3858289, 3858290, 3858291, 3858292, 3858293, 3858294, 3858295, 3858296,
    3858297, 3858298, 3858299, 3858300, 3858301, 3858302, 3858303, 3858304,
    3858305, 3858306, 3858307, 3858308, 3858309, 3858310, 3858311, 3858312,
    3858313, 3858314, 3858315, 3858316, 3858317, 3858318, 3858319, 3858320,
    3858321, 3858322, 3858323, 3858324, 3858325, 3858326, 3858327, 3858328,
    3858329, 3858330, 3858331, 3858332, 3858333, 3858334, 3858335, 3858336,
    3858337, 3858338, 3858339, 3858340, 3858341, 3858342, 3858343, 3858344,
    3858345, 3858346, 3858347, 3858348, 3858349, 3858350, 3858351, 3858352,
    3858353, 3858354, 3858355, 3858356, 3858357, 3858358, 3858359, 3858360,
    3858361, 3858362, 3858363, 3858364, 3858365, 3858366, 3858367, 3858368,
    3858369, 3858370, 3858371, 3858372, 3858373, 3858374, 3858375, 3858376,
    3858377, 3858378, 3858379, 3858380, 3858381, 3858382, 3858383, 3858384,
    3858385, 3858386, 3858387, 3858388, 3858389, 3858390, 3858391, 3858392,
    3858393, 3858394, 3858395, 3858396, 3858397, 3858398, 3858399, 3858400,
    3858401, 3858402, 3858403, 3858404, 3858405, 3858406, 3858407, 3858408,
    3858409, 3858410, 3858411, 3858412, 3858413, 3858415, 3858416, 3858417,
    3858418, 3858419, 3858420, 3858421, 3858422, 3858423, 3858424, 3858425,
    3858426, 3858427, 3858428, 3858429, 3858430, 3858431, 3858432, 3858433,
    3858434, 3858435, 3858436, 3858437, 3858438, 3858439, 3858440, 3858441,
    3858442, 3858443, 3858444, 3858445, 3858446, 3858448, 3858449, 3858450,
    3858451, 3858452, 3858453, 3858454, 3858455, 3858456, 3858457, 3858458,
    3858459, 3858460, 3858461, 3858462, 3858463, 3858464, 3858465, 3858467,
    3858468, 3858469, 3858470, 3858471, 3858472, 3858473, 3858474, 3858475,
    3858476, 3858477, 3858478, 3858479, 3858480, 3858481, 3858482, 3858483,
    3858484, 3858485, 3858486, 3858487, 3858488, 3858489, 3858490, 3858491,
    3858492, 3858493, 3858494, 3858495, 3858496, 3858497, 3858498, 3858499,
    3858500, 3858501, 3858502, 3858503, 3858504, 3858505, 3858506, 3858507,
    3858508, 3858509, 3858510, 3858511, 3858512, 3858513, 3858514, 3858515,
    3858516, 3858517, 3858518, 3858519, 3858520, 3858521, 3858523, 3858524,
    3858525, 3858526, 3858527, 3858528, 3858529, 3858530, 3858531, 3858532,
    3858533, 3858534, 3858535, 3858536, 3858537, 3858538, 3858539, 3858540,
    3858541, 3858542, 3858543, 3858544, 3858545, 3858546, 3858547, 3858548,
    3858549, 3858550, 3858551, 3858552, 3858553, 3858554, 3858555, 3858556,
    3858557, 3858558, 3858559, 3858560, 3858561, 3858562, 3858563, 3858564,
    3858565, 3858566, 3858567, 3858568, 3858569, 3858570, 3858571, 3858572,
    3858573, 3858574, 3858575, 3858576, 3858577, 3858578, 3858579, 3858580,
    3858581, 3858582, 3858583, 3858584, 3858585, 3858586, 3858587, 3858588,
    3858589, 3858590, 3858591, 3858592, 3858593, 3858594, 3858595, 3858596,
    3858597, 3858598, 3858599, 3858600, 3858601, 3858602, 3858603, 3858604,
    3858605, 3858606, 3858607, 3858608, 3858609, 3858610, 3858611, 3858612,
    3858613, 3858614, 3858615, 3858616, 3858617, 3858618, 3858619, 3858620,
    3858621, 3858622, 3858623, 3858624, 3858625, 3858626, 3858627, 3858628,
    3858629, 3858630, 3858631, 3858632, 3858633, 3858634, 3858635, 3858636,
    3858637, 3858638, 3858639, 3858640, 3858641, 3858642, 3858643, 3858644,
    3858645, 3858646, 3858647, 3858648, 3858649, 3858650, 3858651, 3858652,
    3858653, 3858654, 3858655, 3858656, 3858657, 3858658, 3858659, 3858660,
    3858661, 3858662, 3858663, 3858664, 3858665, 3858666, 3858667, 3858668,
    3858669, 3858670, 3858671, 3858672, 3858673, 3858674, 3858675, 3858676,
    3858677, 3858678, 3858679, 3858680, 3858681, 3858682, 3858683, 3858684,
    3858685, 3858686, 3858687, 3858688, 3858689, 3858690, 3858691, 3858692,
    3858693, 3858694, 3858695, 3858696, 3858697, 3858698, 3858699, 3858700,
    3858701, 3858702, 3858703, 3858704, 3858705, 3858706, 3858707, 3858708,
    3858709, 3858710, 3858711, 3858712, 3858713, 3858714, 3858715, 3858716,
    3858717, 3858718, 3858719, 3858720, 3858721, 3858722, 3858723, 3858724,
    3858725, 3858726, 3858727, 3858728, 3858729, 3858730, 3858731, 3858732,
    3858733, 3858734, 3858735, 3858736, 3858737, 3858738, 3858739, 3858740,
    3858741, 3858742, 3858743, 3858744, 3858745, 3858746, 3858747, 3858748,
    3858749, 3858750, 3858751, 3858752, 3858753, 3858754, 3858755, 3858756,
    3858757, 3858758, 3858759, 3858760, 3858761, 3858762, 3858763, 3858764,
    3858765, 3858766, 3858767, 3858768, 3858769, 3858770, 3858771, 3858772,
    3858773, 3858774, 3858775, 3858776, 3858777, 3858778, 3858779, 3858780,
    3858781, 3858782, 3858783, 3858784, 3858785, 3858786, 3858787, 3858788,
    3858789, 3858790, 3858791, 3858792, 3858793, 3858794, 3858795, 3858796,
    3858797, 3858798, 3858799, 3858800, 3858801, 3858802, 3858803, 3858804,
    3858805, 3858806, 3858807, 3858808, 3858809, 3858810, 3858811, 3858812,
    3858813, 3858814, 3858815, 3858816, 3858817, 3858818, 3858819, 3858820,
    3858821, 3858822, 3858823, 3858824, 3858825, 3858826, 3858827, 3858828,
    3858829, 3858830, 3858831, 3858832, 3858833, 3858834, 3858835, 3858836,
    3858837, 3858838, 3858839, 3858840, 3858841, 3858842, 3858843, 3858844,
    3858845, 3858846, 3858847, 3858848, 3858849, 3858850, 3858851, 3858852,
    3858853, 3858854, 3858855, 3858856, 3858857, 3858858, 3858859, 3858860,
    3858861, 3858862, 3858863, 3858864, 3858865, 3858866, 3858867, 3858868,
    3858869, 3858870, 3858871, 3858872, 3858873, 3858874, 3858875, 3858876,
    3858877, 3858878, 3858879, 3858880, 3858881, 3858882, 3858883, 3858884,
    3858885, 3858886, 3858887, 3858888, 3858889, 3858890, 3858891, 3858892,
    3858893, 3858894, 3858895, 3858896, 3858897, 3858898, 3858899, 3858900,
    3858901, 3858902, 3858903, 3858904, 3858905, 3858906, 3858907, 3858908,
    3858909, 3858910, 3858911, 3858912, 3858913, 3858914, 3858915, 3858916,
    3858917, 3858918, 3858919, 3858920, 3858921, 3858922, 3858923, 3858924,
    3858925, 3858926, 3858927, 3858928, 3858929, 3858930, 3858931, 3858932,
    3858933, 3858934, 3858935, 3858936, 3858937, 3858938, 3858939, 3858940,
    3858941, 3858942, 3858943, 3858944, 3858945, 3858946, 3858947, 3858948,
    3858949, 3858950, 3858951, 3858952, 3858953, 3858954, 3858955, 3858956,
    3858957, 3858958, 3858959, 3858960, 3858961, 3858962, 3858963, 3858964,
    3858965, 3858966, 3858967, 3858968, 3858969, 3858970, 3858971, 3858972,
    3858973, 3858974, 3858975, 3858976, 3858977, 3858978, 3858979, 3858981,
    3858982, 3858983, 3858984, 3858985, 3858986, 3858987, 3858988, 3858989,
    3858990, 3858991, 3858992, 3858993, 3858994, 3858995, 3858996, 3858997,
    3858998, 3858999, 3859000, 3859001, 3859002, 3859003, 3859004, 3859005,
    3859006, 3859007, 3859008, 3859009, 3859010, 3859011, 3859012, 3859013,
    3859014, 3859015, 3859016, 3859017, 3859018, 3859019, 3859020, 3859021,
    3859022, 3859023, 3859024, 3859025, 3859026, 3859027, 3859028, 3859029,
    3859030, 3859031, 3859032, 3859033, 3859034, 3859035, 3859036, 3859037,
    3859038, 3859039, 3859040, 3859041, 3859042, 3859043, 3859044, 3859045,
    3859046, 3859047, 3859049, 3859050, 3859051, 3859052, 3859053, 3859054,
    3859055, 3859056, 3859057, 3859058, 3859059, 3859060, 3859061, 3859062,
    3859063, 3859064, 3859065, 3859066, 3859067, 3859068, 3859069, 3859070,
    3859071, 3859072, 3859073, 3859074, 3859075, 3859076, 3859077, 3859078,
    3859079, 3859080, 3859081, 3859082, 3859083, 3859084, 3859085, 3859086,
    3859087, 3859088, 3859089, 3859090, 3859091, 3859092, 3859093, 3859094,
    3859095, 3859096, 3859097, 3859098, 3859099, 3859100, 3859101, 3859102,
    3859103, 3859105, 3859106, 3859107, 3859108, 3859109, 3859110, 3859111,
    3859112, 3859113, 3859114, 3859115, 3859116, 3859117, 3859118, 3859119,
    3859120, 3859122, 3859123, 3859124, 3859125, 3859126, 3859127, 3859128,
    3859129, 3859130, 3859131, 3859132, 3859133, 3859134, 3859135, 3859136,
    3859137, 3859138, 3859139, 3859140, 3859141, 3859142, 3859143, 3859144,
    3859145, 3859146, 3859147, 3859148, 3859149, 3859150, 3859151, 3859152,
    3859153, 3859154, 3859155, 3859156, 3859157, 3859158, 3859159, 3859160,
    3859161, 3859162, 3859163, 3859165, 3859166, 3859167, 3859168, 3859169,
    3859172, 3859173, 3859174, 3859175, 3859176, 3859177, 3859178, 3859179,
    3859180, 3859181, 3859182, 3859183, 3859184, 3859185, 3859186, 3859187,
    3859188, 3859189, 3859190, 3859191, 3859192, 3859193, 3859194, 3859195,
    3859196, 3859197, 3859199, 3859200, 3859201, 3859202, 3859203, 3859204,
    3859205, 3859206, 3859207, 3859208, 3859209, 3859210, 3859211, 3859212,
    3859213, 3859214, 3859215, 3859216, 3859217, 3859218, 3859219, 3859220,
    3859221, 3859222, 3859223, 3859224, 3859225, 3859226, 3859227, 3859228,
    3859229, 3859230, 3859231, 3859232, 3859233, 3859234, 3859235, 3859236,
    3859237, 3859238, 3859239, 3859240, 3859241, 3859242, 3859243, 3859244,
    3859245, 3859246, 3859247, 3859248, 3859249, 3859250, 3859251, 3859252,
    3859253, 3859254};

void PBFS(GraphEngine &graph_engine,
          node_id_t source,
          node_id_t maxNodeID,
          adjlist &nbd,
          pvector<CountT> &path_counts,
          Bitmap &succ,
          vector<SlidingQueue<node_id_t>::iterator> &depth_index,
          SlidingQueue<node_id_t> &queue)
{
    pvector<node_id_t> depths(maxNodeID, -1);
    depths[source] = 0;
    path_counts[source] = 1;
    queue.push_back(source);
    depth_index.push_back(queue.begin());
    queue.slide_window();
    node_id_t g_out_start = nbd.node_id;
#pragma omp parallel
    {
        node_id_t depth = 0;
        QueueBuffer<node_id_t> lqueue(queue);
        while (!queue.empty())
        {
            depth++;

#pragma omp for schedule(dynamic, 64) nowait
            for (auto q_iter = queue.begin(); q_iter < queue.end(); q_iter++)
            {
                GraphBase *g_ = graph_engine.create_graph_handle();
                node_id_t u = *q_iter;
                for (node_id_t &v : g_->get_out_nodes_id(u))
                {
                    if ((depths[v] == -1) &&
                        (compare_and_swap(
                            depths[v], static_cast<node_id_t>(-1), depth)))
                    {
                        lqueue.push_back(v);
                    }
                    if (depths[v] == depth)
                    {
                        assert(v >= g_out_start);
                        succ.set_bit_atomic(v - g_out_start);
#pragma omp atomic
                        path_counts[v] += path_counts[u];
                    }
                }
                g_->close(false);
            }
            lqueue.flush();
#pragma omp barrier
#pragma omp single
            {
                depth_index.push_back(queue.begin());
                queue.slide_window();
            }
        }
    }
    depth_index.push_back(queue.begin());
}

void PrintStep(const std::string &s, double seconds, int64_t count = -1)
{
    if (count != -1)
        printf("%5s%11" PRId64 "  %10.5lf\n", s.c_str(), count, seconds);
    else
        printf("%5s%23.5lf\n", s.c_str(), seconds);
}

pvector<ScoreT> Brandes(GraphEngine &graph_engine,
                        std::vector<node_id_t> &random_nodes,
                        node_id_t maxNodeID,
                        node_id_t num_nodes,
                        edge_id_t num_edges,
                        int num_iters)
{
    Times t;
    t.start();
    GraphBase *g = graph_engine.create_graph_handle();
    pvector<ScoreT> scores(maxNodeID, 0);
    pvector<CountT> path_counts(maxNodeID);
    Bitmap succ(num_edges);
    vector<SlidingQueue<node_id_t>::iterator> depth_index;
    SlidingQueue<node_id_t> queue(maxNodeID);
    t.stop();
    //PrintStep("a", t.t_secs());
    OutCursor *out_cur = g->get_outnbd_iter();
    out_cur->set_key_range({0, num_nodes});
    adjlist nbd;
    out_cur->next(&nbd);
    node_id_t g_out_start = nbd.node_id;

    for (int iter = 0; iter < 10; iter++)
    {
        node_id_t source = random_nodes.at(iter);
        t.start();
        path_counts.fill(0);
        depth_index.resize(0);
        queue.reset();
        succ.reset();
        PBFS(graph_engine,
             source,
             maxNodeID,
             nbd,
             path_counts,
             succ,
             depth_index,
             queue);
        //t.stop();
        //PrintStep("b", t.t_secs());
        pvector<ScoreT> deltas(maxNodeID, 0);
        //t.start();
        for (int d = depth_index.size() - 2; d >= 0; d--)
        {
#pragma omp parallel for schedule(dynamic, 64)
            for (auto it = depth_index[d]; it < depth_index[d + 1]; it++)
            {
                GraphBase *g_ = graph_engine.create_graph_handle();
                node_id_t u = *it;
                ScoreT delta_u = 0;
                for (node_id_t v : g_->get_out_nodes_id(u))
                {
                    assert(v >= g_out_start);
                    if (succ.get_bit(v - g_out_start))
                    {
                        delta_u +=
                            (path_counts[u] / path_counts[v]) * (1 + deltas[v]);
                    }
                }
                deltas[u] = delta_u;
                scores[u] += delta_u;
                g_->close(false);
            }
        }
        t.stop();
        //PrintStep("p", t.t_secs());
    }

    // normalize scores
    ScoreT biggest_score = 0;
#pragma omp parallel for reduction(max : biggest_score)
    for (node_id_t n = 0; n < maxNodeID; n++)
        biggest_score = max(biggest_score, scores[n]);

#pragma omp parallel for
    for (node_id_t n = 0; n < maxNodeID; n++)
        scores[n] = scores[n] / biggest_score;

    return scores;
}

// Returns k pairs with largest values from list of key-value pairs
template <typename KeyT, typename ValT>
std::vector<std::pair<ValT, KeyT>> TopK(
    const std::vector<std::pair<KeyT, ValT>> &to_sort, size_t k)
{
    std::vector<std::pair<ValT, KeyT>> top_k;
    ValT min_so_far = 0;
    for (auto kvp : to_sort)
    {
        if ((top_k.size() < k) || (kvp.second > min_so_far))
        {
            top_k.push_back(std::make_pair(kvp.second, kvp.first));
            std::sort(top_k.begin(),
                      top_k.end(),
                      std::greater<std::pair<ValT, KeyT>>());
            if (top_k.size() > k) top_k.resize(k);
            min_so_far = top_k.back().first;
        }
    }
    return top_k;
}

void print_top_scores(GraphBase *g,
                      node_id_t maxNodeID,
                      const pvector<ScoreT> &scores)
{
    vector<pair<node_id_t, ScoreT>> score_pairs(maxNodeID);
    NodeCursor *node_cursor = g->get_node_iter();

    node found = {0};
    node_cursor->next(&found);
    while (found.id != UINT32_MAX)
    {
        score_pairs.emplace_back(found.id, scores[found.id]);
        node_cursor->next(&found);
    }
    node_id_t k = 100;
    vector<pair<ScoreT, node_id_t>> top_k = TopK(score_pairs, k);
    node_id_t it = 0;
    std::cout << "Top " << k << " nodes by PageRank:" << std::endl;
    for (auto kvp : top_k)
    {
        it++;
        //std::cout << kvp.second << ":" << kvp.first << std::endl;
    }
}

int main(int argc, char *argv[])
{
    std::cout << "Running BC" << std::endl;
    CmdLineApp cli(argc, argv);
    if (!cli.parse_args())
    {
        return -1;
    }

    cmdline_opts opts = cli.get_parsed_opts();
    opts.stat_log += "/" + opts.db_name;
    std::vector<node_id_t> random_nodes;
    Times t;
    t.start();
    GraphEngine graphEngine(THREAD_NUM, opts);
    graphEngine.calculate_thread_offsets();
    GraphBase *graph = graphEngine.create_graph_handle();
    t.stop();
    std::cout << "Graph loaded in " << t.t_secs() << std::endl;

    if (opts.start_vertex == -1)
    {
        graph->get_random_node_ids(random_nodes, opts.num_trials);
    }
    else
    {
        random_nodes.push_back(opts.start_vertex);
        opts.num_trials = 1;
    }

    node_id_t maxNodeID = graph->get_max_node_id();
    node_id_t num_edges = graph->get_num_edges();
    graph->close(false);

    long double total_time = 0;
    sssp_info info(0);
    for (int i = 0; i < opts.num_trials; i++)
    {
        t.start();
        pvector<ScoreT> scores = Brandes(graphEngine,
                                         workloads,
                                         maxNodeID,
                                         num_nodes,
                                         num_edges,
                                         opts.iterations);
        

        t.stop();

        info.time_taken = t.t_secs();
        total_time += info.time_taken;
        std::cout << "BC completed in : "
                  << info.time_taken << std::endl;
        print_top_scores(graph, maxNodeID, scores);
        print_csv_info(opts.db_name, info, opts.stat_log);
    }
    std::cout << "Average time taken for " << opts.num_trials
              << " trials: " << total_time / opts.num_trials << std::endl;

    graphEngine.close_graph();
    return 0;
}