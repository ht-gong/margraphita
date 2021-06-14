#include "adj_list.h"
#include "common.h"
#include <filesystem>
#include <cstring>
#include <cassert>
#include <iostream>
#include <string>
#include <cstring>
#include <cassert>
#include <unordered_map>
#include <wiredtiger.h>

using namespace std;
const std::string GRAPH_PREFIX = "adj";

AdjList::AdjList() {}

AdjList::AdjList(opt_args opt_params)

{
    this->create_new = opt_params.create_new;
    this->read_optimize = opt_params.read_optimize;
    this->is_directed = opt_params.is_directed;
    this->is_weighted = opt_params.is_weighted;
    this->db_name = opt_params.db_name;

    try
    {
        CommonUtil::check_graph_params(opt_params);
    }
    catch (GraphException G)
    {
        std::cout << G.what() << std::endl;
    }

    if (opt_params.create_new == false)
    {
        throw GraphException("Missing param " + CREATE_NEW);
    }
    if (opt_params.create_new)
    {
        create_new_graph();
        if (opt_params.optimize_create == false)
        {
            /**
       *  Create indices with graph.
       * NOTE: If user opts to optimize graph creation, then they're
       * responsible for calling create_indices() AFTER all the
       * nodes/edges have been added to graph.
       * This is done to improve performance of bulk-loading.
       */
        }
    }
    else
    {
        __restore_from_db(db_name); //! APT Check towards the end by a unit test
    }
}

/**
 * @brief Returns the metadata associated with the key param from the METADATA
 * table.
 * Same from standard_graph implementation
 */
string AdjList::get_metadata(string key)
{

    int ret = 0;
    if (this->metadata_cursor == NULL)
    {
        if ((ret = _get_table_cursor(METADATA, &metadata_cursor, false)) != 0)
        {
            fprintf(stderr, "Failed to create cursor to the metadata table.");
            exit(-1);
        }
    }
    metadata_cursor->set_key(metadata_cursor, key.c_str());
    ret = metadata_cursor->search(metadata_cursor);
    if (ret != 0)
    {
        fprintf(stderr, "Failed to retrieve metadata for the key %s", key.c_str());
        exit(-1);
    }

    const char *value;
    ret = metadata_cursor->get_value(metadata_cursor, &value);
    metadata_cursor->close(metadata_cursor);

    return string(value);
}

/**
 * @brief This function is used to check if a node identified by node_id exists
 * in the node table.
 *
 * @param node_id the node_id to be searched.
 * @return true if the node is found; false otherwise.
 */
bool AdjList::has_node(int node_id)
{
    int ret = 0;
    if (this->node_cursor == NULL)
    {
        ret = _get_table_cursor(NODE_TABLE, &(this->node_cursor), false);
    }
    this->node_cursor->set_key(this->node_cursor, node_id);
    ret = this->node_cursor->search(this->node_cursor);
    if (ret == 0)
    {
        return true;
    }
    else
    {
        return false;
    }
}

int AdjList::get_num_nodes()
{
    int ret = 0;
    WT_CURSOR *cursor = nullptr;
    ret = _get_table_cursor(NODE_TABLE, &cursor, false);

    int count = 0;
    while ((ret = cursor->next(cursor)) == 0)
    {
        count += 1;
    }
    cursor->close(cursor);
    return count;
}

int AdjList::get_num_edges()
{
    int ret = 0;
    WT_CURSOR *cursor = nullptr;
    ret = _get_table_cursor(EDGE_TABLE, &cursor, false);

    int count = 0;
    while ((ret = cursor->next(cursor)) == 0)
    {
        count += 1;
    }
    cursor->close(cursor);
    return count;
}

//#if 0
/**
 * @brief This private function inserts metadata values into the metadata
 * table. The fields are self explanatory.
 * Same from standard_graph implementation.
 */
void AdjList::insert_metadata(string key, char *value)
{
    int ret = 0;

    if ((ret = _get_table_cursor(METADATA, &metadata_cursor, false)) != 0)
    {
        fprintf(stderr, "Failed to create cursor to the metadata table.");
        exit(-1);
    }

    this->metadata_cursor->set_key(metadata_cursor, key.c_str());
    this->metadata_cursor->set_value(metadata_cursor, value);
    if ((ret = this->metadata_cursor->insert(metadata_cursor)) != 0)
    {
        fprintf(stderr, "failed to insert metadata for key %s", key.c_str());
        // TODO(puneet): Maybe create a GraphException?
    }
    this->metadata_cursor->close(metadata_cursor);
}
//#endif

void AdjList::create_new_graph()
{
    int ret;
    // Create new directory for WT DB
    std::filesystem::path dirname = "./db/" + db_name;
    if (std::filesystem::exists(dirname))
    {
        filesystem::remove_all(dirname); // remove if exists;
    }
    std::filesystem::create_directories(dirname);

    // open connection to WT
    if (CommonUtil::open_connection(const_cast<char *>(dirname.c_str()), &conn) < 0)
    {
        exit(-1);
    };

    // Open a session handle for the database
    if (CommonUtil::open_session(conn, &session) != 0)
    {
        exit(-1);
    }

    // Initialize edge ID for the edge table
    // AdjList has no edge id in the edge table but we are using the same structure as used by Standardgraph. So, the edge_id value will be -999 for all edges in the AdjList implementation.
    edge_id = -999;

    // Set up the node table
    // The node entry is of the form: <id>,<in_degree>,<out_degree>
    // If the graph is read_optimized, add columns and format for in/out degrees
    if (read_optimize)
    {
        node_columns.push_back(IN_DEGREE);
        node_columns.push_back(OUT_DEGREE);
        node_value_format = "II";
    }
    else
    {
        node_columns.push_back("na"); //have to do this because the column count must match.
        node_value_format = "s";      //1 byte fixed length char[] to hold ""
    }
    // Now Create the Node Table
    //!What happens when the table is not read-optimized? I store "" <-ok?

    CommonUtil::set_table(session, NODE_TABLE, node_columns, node_key_format,
                          node_value_format);

    // ******** Now set up the Edge Table     **************
    // Edge Column Format : <id><src><dst><weight>
    //Now prepare the edge value format. starts with II for src,dst. Add another I if weighted
    if (is_weighted)
    {
        edge_columns.push_back(WEIGHT);
        edge_value_format += "I";
    }

    // Create edge table
    CommonUtil::set_table(session, EDGE_TABLE, edge_columns,
                          edge_key_format, edge_value_format);

    // Create adjlist_in_edges table
    CommonUtil::set_table(session, ADJLIST_IN_TABLE, in_adjlist_columns,
                          adjlist_key_format, adjlist_value_format);

    // Create adjlist_out_edges table
    CommonUtil::set_table(session, ADJLIST_OUT_TABLE, out_adjlist_columns,
                          adjlist_key_format, adjlist_value_format);

    /* Now doing the metadata table creation.
     function This table stores the graph metadata
     value_format:raw byte string (Su)
     key_format: string (S)
  */
    string metadata_table_name = "table:" + string(METADATA);
    if ((ret = session->create(session, metadata_table_name.c_str(),
                               "key_format=S,value_format=S")) > 0)
    {
        fprintf(stderr, "Failed to create the metadata table ");
    }

    if ((ret = _get_table_cursor(METADATA, &metadata_cursor, false)) != 0)
    {
        fprintf(stderr, "Failed to create cursor to the metadata table.");
        exit(-1);
    }

    //# if 0
    // DB_NAME
    string db_name_fmt;
    // char *db_name_packed =
    //     CommonUtil::pack_string_wt(db_name, session, &db_name_fmt);
    // insert_metadata(DB_NAME, db_name_fmt, db_name_packed);
    insert_metadata(DB_NAME, const_cast<char *>(db_name.c_str()));

    // READ_OPTIMIZE
    string read_optimized_str = read_optimize ? "true" : "false";
    insert_metadata(READ_OPTIMIZE, const_cast<char *>(read_optimized_str.c_str()));

    // IS_DIRECTED
    string is_directed_str = is_directed ? "true" : "false";
    insert_metadata(IS_DIRECTED, const_cast<char *>(is_directed_str.c_str()));

    //IS_WEIGHTED
    string is_weighted_str = is_weighted ? "true" : "false";
    insert_metadata(IS_WEIGHTED, const_cast<char *>(is_weighted_str.c_str()));
    //#endif

    this->metadata_cursor->close(this->metadata_cursor);
}

/**
 * @brief This is the generic function to get a cursor on the table
 *
 * @param table This is the table name for which the cursor is needed.
 * @param cursor This is the pointer that will hold the set cursor.
 * @param is_random This is a bool value to indicate if the cursor must be
 * random.
 * @return 0 if the cursor could be set
 */
int AdjList::_get_table_cursor(string table, WT_CURSOR **cursor,
                               bool is_random)
{
    std::string table_name = "table:" + table;
    const char *config = NULL;
    if (is_random)
    {
        config = "next_random=true";
    }

    if (int ret = session->open_cursor(session, table_name.c_str(), NULL, config,
                                       cursor) != 0) //!APT: Check for cursor close.
    {
        fprintf(stderr, "Failed to get table cursor to %s\n", table_name.c_str());
        return ret;
    }
    return 0;
}

/**
 * @brief The information that gets persisted to WT is of the form:
 * <node_id>,in_degree,out_degree.
 * in_degree and out_degree are persisted if read_optimize is true. ints
 *
 *
 * @param to_insert
 */
void AdjList::add_node(node to_insert)
{
    int ret = 0;
    if (node_cursor == NULL)
    {
        ret = _get_table_cursor(NODE_TABLE, &node_cursor, false);
    }
    node_cursor->set_key(node_cursor, to_insert.id);

    if (read_optimize)
    {
        node_cursor->set_value(node_cursor, 0, 0);
    }
    else
    {
        node_cursor->set_value(node_cursor, "");
    }

    ret = node_cursor->insert(node_cursor);

    if (ret != 0)
    {
        throw GraphException("Failed to add node_id" +
                             std::to_string(to_insert.id));
    }
}

/**
 * @brief Add a record for the node_id in the in or out adjlist,
 * as pointed by the cursor.
 * if the node_id record already exists then reset it with an empty list.
**/
void AdjList::add_adjlist(WT_CURSOR *cursor, int node_id)
{
    int ret = 0;
    // Check if the cursor is not NULL, else throw exception
    if (cursor == NULL)
    {
        throw GraphException("Uninitiated Cursor passed to add_adjlist call");
    }

    cursor->set_key(cursor, node_id);

    // Now, initialize the in/out degree to 0 and adjlist to empty list
    cursor->set_value(cursor, node_id, 0, ""); // serialize the vector and send ""
    //TODO: check if "" is acceptable as a const char *
    ret = cursor->insert(cursor);
    if (ret != 0)
    {
        throw GraphException("Failed to add node_id" +
                             std::to_string(node_id));
    }
}

/**
 * @brief Delete the record of the node_id in the in or out 
 * adjlist as pointed by the cursor.
**/
void AdjList::delete_adjlist(WT_CURSOR *cursor, int node_id)
{
    int ret = 0;
    // Check if the cursor is not NULL, else throw exception
    if (cursor == NULL)
    {
        throw GraphException("Uninitiated Cursor passed to delete_adjlist");
    }

    cursor->set_key(cursor, node_id);
    ret = node_cursor->remove(node_cursor);

    if (ret != 0)
    {
        throw GraphException("Could not delete node with ID " + to_string(node_id));
    }
    //!APT still pending... tag to continue from here... change the above code too!!!
    // Delete node from the adjlist node table
    // Delete the node record from adjlist in/out degree table
    // Delete node from the adjlist list for any other nodes and reduce the degree..
}

#if 0

void AdjList::add_edge(edge to_insert)
{
    // Add dst and src nodes if they don't exist.
    if (!has_node(to_insert.src_id))
    {
        node src = {0};
        src.id = to_insert.src_id;
        add_node(src);
    }
    if (!has_node(to_insert.dst_id))
    {
        node dst = {0};
        dst.id = to_insert.dst_id;
        add_node(dst);
    }

    WT_CURSOR *cursor = nullptr;
    int ret = _get_table_cursor(EDGE_TABLE, &cursor, false);

    // check if the edge exists already, if so, get edge_id
    int found_edge_id = get_edge_id(to_insert.src_id, to_insert.dst_id);
    if (found_edge_id > 0)
    {
        // The edge exists, set the cursor to point to that edge_id
        cursor->set_key(cursor, found_edge_id);
    }
    else
    {
        // The edge does not exist, use edge-id = -1, since there is no edge id value in adj list implementation.
        to_insert.id = -1;
        cursor->set_key(cursor, to_insert.src_id, to_insert.dst_id);
        cout << "New Edge ID inserted" << endl;
    }

    if (is_weighted)
    {
        cursor->set_value(cursor, to_insert.edge_weight);
    }
    else
    {
        cursor->set_value(cursor, 0);
    }
    ret = cursor->insert(cursor);
    if (ret != 0)
    {
        throw GraphException("Failed to insert edge (" +
                             to_string(to_insert.src_id) + "," +
                             to_string(to_insert.dst_id));
    }
    cursor->close(cursor);

    // Update the adj_list table both in and out
    // !APT : Write helper functions before updating adj_lists

    // If read_optimized is true, we update in/out degreees in the node table.
    if (this->read_optimize)
    {
        // update in/out degrees for src node in NODE_TABLE
        ret = _get_table_cursor(NODE_TABLE, &cursor, false);
        CommonUtil::check_return(ret, "Failed to open a cursor to the node table");

        cursor->set_key(cursor, to_insert.src_id);
        cursor->search(cursor); //TODO: do a check
        node found = __record_to_node(cursor);
        found.id = to_insert.src_id;
        found.out_degree = found.out_degree + 1;
        update_node_degree(cursor, found.id, found.in_degree,
                           found.out_degree); //! pass the cursor to this function

        // update in/out degrees for the dst node in the NODE_TABLE
        cursor->reset(cursor);
        cursor->set_key(cursor, to_insert.dst_id);
        cursor->search(cursor);
        found = __record_to_node(cursor);
        found.id = to_insert.dst_id;
        found.in_degree = found.in_degree + 1;
        update_node_degree(cursor, found.id, found.in_degree, found.out_degree);
        cursor->close(cursor);
    }
}

node AdjList::__record_to_node(WT_CURSOR *cursor)
{
    node found = {0};
    found.id = cursor->get_key(cursor);
    if (read_optimize)
    {
        cursor->get_value(cursor, &found.in_degree, &found.out_degree);
    }
    return found;
}

node AdjList::get_random_node()
{
    node found;
    WT_CURSOR *cursor;
    int ret = _get_table_cursor(NODE_TABLE, &cursor, true);
    if (ret != 0)
    {
        throw GraphException("could not get a random cursor to the node table");
    }
    ret = cursor->next(cursor);
    if (ret != 0)
    {
        throw GraphException("Could not seek a random node in the table");
    }
    found = AdjList::__record_to_node(cursor);
    cursor->close(cursor);
    return found;
}

void AdjList::close()
{
    //TODO:check if there is any state that needs to be saved in the metadata
    //table.
    CommonUtil::close_connection(conn);
}

void AdjList::delete_node(int node_id)
{
    WT_CURSOR *cursor;
    int ret = _get_table_cursor(NODE_TABLE, &cursor, false);
    if (ret != 0)
    {
        throw GraphException("Could not get a cursor to the NODE table");
    }
    cursor->set_key(cursor, node_id);
    ret = cursor->remove(cursor);
    if (ret != 0)
    {
        throw GraphException("failed to delete node with ID " + std::to_string(node_id));
    }
    //Delete Edges
    delete_related_edges(cursor, node_id);

    //delete ADJ_INLIST_TABLE entries
    delete_adjlist(cursor, node_id);

    //delete ADJ_OUTLIST_TABLE entrties
    delete_adjlist(cursor, node_id);
}

/**
 * @brief Get the in degree for the node provided  
 * 
 * @param node_id ID for which in_degree is required
 * @return int in degree of the node node_id
 */
int AdjList::get_in_degree(int node_id)
{
    int ret = 0;
    if (read_optimize)
    {
        if (node_cursor == nullptr)
        {
            ret = _get_table_cursor(NODE_TABLE, &node_cursor, false);
            if (ret != 0)
            {
                throw GraphException("Could not get a cursor to the Node table");
            }
        }

        node_cursor->set_key(node_cursor, node_id);
        ret = node_cursor->search(node_cursor);
        if (ret != 0)
        {
            throw GraphException("Could not find a node with ID " + std::to_string(node_id));
        }
        node found = __record_to_node(node_cursor);
        return found.in_degree;
    }
    else
    {
        if (in_adjlist_cursor == nullptr)
        {
            ret = _get_table_cursor(ADJ_INLIST_TABLE, &in_adjlist_cursor, false);
            if (ret != 0)
            {
                throw GraphException("Could not get a cursor to the in_adjlist table");
            }
        }
        in_adjlist_cursor->set_key(in_adjlist_cursor, node_id);
        ret = in_adjlist_cursor->search(in_adjlist_cursor);
        if (ret != 0)
        {
            throw GraphException("Could not find node with ID" + std::to_string(node_id) + " in the adjlist");
        }
        adjlist in_edges = __record_to_adjlist(in_adjlist_cursor);

        return in_edges.degree;
    }
}

/**
 * @brief Get the out degree for the node requested
 * 
 * @param node_id The ID of the node for which the degree is sought
 * @return int the node degree for the node with ID node_id. 
 */
int AdjList::get_out_degree(int node_id)
{
    int ret = 0;
    if (read_optimize)
    {
        if (node_cursor == nullptr)
        {
            ret = _get_table_cursor(NODE_TABLE, &node_cursor, false);
            if (ret != 0)
            {
                throw GraphException("Could not get a node table cursor");
            }
        }

        node_cursor->set_key(node_cursor, node_id);
        ret = node_cursor->search(node_cursor);
        if (ret != 0)
        {
            throw GraphException("Could not find a node with ID " + std::to_string(node_id));
        }
        node found = __record_to_node(node_cursor);
        return found.out_degree;
    }
    else
    {
        if (out_adjlist_cursor == nullptr)
        {
            ret = _get_table_cursor(ADJ_OUTLIST_TABLE, &out_adjlist_cursor, false);
            if (ret != 0)
            {
                throw GraphException("Could not find a cursor to the out adjlist table");
            }
        }
        out_adjlist_cursor->set_key(out_adjlist_cursor, node_id);
        ret = out_adjlist_cursor->search(out_adjlist_cursor);
        if (ret != 0)
        {
            throw GraphException("Could not find a node with ID " + std::to_string(node_id) + " in the adjlist");
        }
        adjlist out_edges = __record_to_adjlist(out_adjlist_cursor);
        return out_edges.degree;
    }
}

//TODO:Verify that this works. get value should handle the buffer size.
/**
 * @brief This function converts the record the cursor passed points to into a
 * adjlist struct
 * 
 * @param cursor the cursor set to the record which needs to be read
 * @return adjlist the found adjlist struct.
 */
adjlist AdjList::__record_to_adjlist(WT_CURSOR *cursor)
{
    adjlist found;
    char *packed_vec;

    cursor->get_value(cursor, &found.degree, packed_vec);
    std::string str(packed_vec);
    found.edgelist = CommonUtil::unpack_int_vector_std(str);
    found.degree = found.edgelist.size();

    return found;
}

/**
 * @brief Get all nodes in the graph
 * 
 * @return std::vector<node> vector of all nodes
 */
std::vector<node> AdjList::get_nodes()
{
    std::vector<node> nodelist;
    int ret = 0;
    if (node_cursor == nullptr)
    {
        ret = _get_table_cursor(NODE_TABLE, &node_cursor, false);
        if (ret != 0)
        {
            throw GraphException("Could not open a cursor to the node table");
        }
    }
    while ((ret = node_cursor->next(node_cursor) == 0))
    {
        node found = __record_to_node(node_cursor);
        nodelist.push_back(found);
    }
    return nodelist;
}

/**
 * @brief Get a list of all the edges in the graph
 * 
 * @return std::vector<edge> Vector containing all the edges in the graph
 */
std::vector<edge> AdjList::get_edges()
{
    std::vector<edge> edgelist;

    int ret = 0;
    if (edge_cursor == nullptr)
    {
        ret = _get_table_cursor(EDGE_TABLE, &edge_cursor, false);
        if (ret != 0)
        {
            throw GraphException("Could not oppen a cursor to the edge table");
        }
    }
    while ((ret = edge_cursor->next(edge_cursor) == 0))
    {
        edge found = __record_to_edge(edge_cursor);
        edgelist.push_back(found);
    }
    return edgelist;
}

/**
 * @brief Get the edge identified by (src_id, dst_id)
 * 
 * @param src_id source id
 * @param dst_id destination id
 * @return edge edge identified by (src,dst) pair
 */
edge AdjList::get_edge(int src_id, int dst_id)
{

    edge found = {0};
    int ret = 0;

    if (edge_cursor == nullptr)
    {
        ret = _get_table_cursor(EDGE_TABLE, &edge_cursor, false);
        if (ret != 0)
        {
            throw GraphException("Could not oppen a cursor to the edge table");
        }
    }
    edge_cursor->set_key(edge_cursor, src_id, dst_id);
    ret = edge_cursor->search(edge_cursor);
    if (ret != 0)
    {
        return found;
    }
    else
    {
        return __record_to_edge(edge_cursor);
    }
}

/**
 * @brief Check if an edge (srd_id, dst_id) exists in the graph
 *  
 * @param src_id source id
 * @param dst_id destination id
 * @return true if the edge exists
 * @return false if the edge does not exist
 */
bool AdjList::has_edge(int src_id, int dst_id)
{
    edge found = {0};
    int ret = 0;
    bool val;

    if (edge_cursor == nullptr)
    {
        ret = _get_table_cursor(EDGE_TABLE, &edge_cursor, false);
        if (ret != 0)
        {
            throw GraphException("Could not oppen a cursor to the edge table");
        }
    }
    edge_cursor->set_key(edge_cursor, src_id, dst_id);
    ret = edge_cursor->search(edge_cursor);
    return (ret == 0); //true if found :)
}