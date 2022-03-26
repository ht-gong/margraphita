#include "reader.h"

#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include "bulk_insert.h"
#include "common.h"
namespace reader
{

std::vector<edge> parse_edge_entries(std::string filename)
{
    std::vector<edge> edges;
    std::ifstream newfile(filename);

    if (newfile.is_open())
    {
        std::string tp;
        while (getline(newfile, tp))
        {
            if ((tp.compare(0, 1, "#") == 0) or (tp.compare(0, 1, "%") == 0) or
                (tp.empty()))
            {
                continue;
            }

            else
            {
                int a, b;
                std::stringstream s_stream(tp);
                s_stream >> a;
                s_stream >> b;
                edge to_insert = {0};
                to_insert.src_id = a;
                to_insert.dst_id = b;

                edges.push_back(to_insert);
                lock.lock();
                in_adjlist[b].push_back(a);   // insert a in b's in_adjlist
                out_adjlist[a].push_back(b);  // insert b in a's out_adjlist
                lock.unlock();
            }
        }
        newfile.close();
    }
    else
    {
        std::cout << "**could not open " << filename << std::endl;
    }
    return edges;
}

std::vector<node> parse_node_entries(std::string filename)
{
    std::vector<node> nodes;
    std::ifstream newfile(filename);

    if (newfile.is_open())
    {
        std::string tp;
        while (getline(newfile, tp))
        {
            if ((tp.compare(0, 1, "#") == 0) or (tp.compare(0, 1, "%") == 0) or
                (tp.empty()))
            {
                continue;
            }
            else
            {
                node to_insert;
                std::stringstream s_str(tp);
                s_str >> to_insert.id;

                try
                {
                    to_insert.in_degree = in_adjlist.at(to_insert.id).size();
                }
                catch (const std::out_of_range &oor)
                {
                    to_insert.in_degree = 0;
                }

                try
                {
                    to_insert.out_degree = out_adjlist.at(to_insert.id).size();
                }
                catch (const std::out_of_range &oor)
                {
                    to_insert.out_degree = 0;
                }

                nodes.push_back(to_insert);
            }
        }
        newfile.close();
    }
    else
    {
        std::cout << "** could not open " << filename << std::endl;
    }
    return nodes;
}

// std::unordered_map<int, node> parse_node_entries(std::string filename)
// {
//     std::unordered_map<int, node> nodes;
//     std::vector<std::string> filenames;

//     std::string name = filename + "_indeg_";
//     filenames.push_back(name);

//     name = filename + "_outdeg_";
//     filenames.push_back(name);
//     int i = 0;
//     for (std::string file : filenames)
//     {

//         std::cout << "\n Now reading " << file << std::endl;
//         std::ifstream newfile(file);
//         if (newfile.is_open())
//         {
//             std::string tp;
//             while (getline(newfile, tp))
//             {
//                 if ((tp.compare(0, 1, "#") == 0) or (tp.compare(0, 1, "%") ==
//                 0) or (tp.empty()))
//                 {
//                     continue;
//                 }

//                 else
//                 {
//                     int a, b;
//                     std::stringstream s_stream(tp);
//                     s_stream >> a;
//                     s_stream >> b;

//                     auto search = nodes.find(b);
//                     if (search == nodes.end())
//                     {
//                         node to_insert;
//                         to_insert.id = b; //Second entry is node id
//                         if (i == 0)
//                         {
//                             to_insert.in_degree = a;
//                         }
//                         else
//                         {
//                             to_insert.out_degree = a;
//                         }
//                         nodes[b] = to_insert;
//                     }
//                     else
//                     {
//                         node to_insert = search->second;
//                         if (i == 0)
//                         {
//                             to_insert.in_degree = a;
//                         }
//                         else
//                         {
//                             to_insert.out_degree = a;
//                         }
//                         nodes[b] = to_insert;
//                     }
//                 }
//             }
//             newfile.close();
//         }
//         else
//         {
//             std::cout << "could not open";
//         }
//         i++;
//     }

//     return nodes;
// }
}  // namespace reader