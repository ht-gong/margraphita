
#include "graph.h"

#include <wiredtiger.h>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <iostream>
#include <string>
#include <unordered_map>

#include "common.h"
#include "graph_exception.h"
using namespace std;
GraphBase::GraphBase(graph_opts opt_params)
{
    opts = opt_params;

    try
    {
        CommonUtil::check_graph_params(opts);
    }
    catch (GraphException &G)
    {
        std::cout << G.what() << std::endl;
    }
}

static void create_new_graph(graph_opts &opts, WT_CONNECTION *conn)
{
    WT_SESSION *session;
    if (CommonUtil::open_session(conn, &session) != 0)
    {
        throw GraphException("Cannot open session");
    }

    /* Now doing the metadata table creation.
    function This table stores the graph metadata
    value_format:string (S)
    key_format: string (S)
    */
    string metadata_table_name = "table:" + string(METADATA);
    if (session->create(session,
                        metadata_table_name.c_str(),
                        "key_format=S,value_format=S") > 0)
    {
        fprintf(stderr, "Failed to create the metadata table ");
    }
    WT_CURSOR *metadata_cursor;
    int ret = GraphBase::_get_table_cursor(
        METADATA, &metadata_cursor, session, false);
    if (ret != 0)
    {
        throw GraphException("Could not get a metadata cursor");
    }

    // DB_NAME
    string db_name_fmt;
    GraphBase::insert_metadata(
        DB_NAME, const_cast<char *>(opts.db_name.c_str()), metadata_cursor);

    // DB_DIR
    GraphBase::insert_metadata(
        DB_DIR, const_cast<char *>(opts.db_dir.c_str()), metadata_cursor);

    // READ_OPTIMIZE
    string read_optimized_str = opts.read_optimize ? "true" : "false";
    GraphBase::insert_metadata(READ_OPTIMIZE,
                               const_cast<char *>(read_optimized_str.c_str()),
                               metadata_cursor);

    // IS_DIRECTED
    string is_directed_str = opts.is_directed ? "true" : "false";
    GraphBase::insert_metadata(IS_DIRECTED,
                               const_cast<char *>(is_directed_str.c_str()),
                               metadata_cursor);

    // is_weighted
    string is_weighted_str = opts.is_weighted ? "true" : "false";
    GraphBase::insert_metadata(IS_WEIGHTED,
                               const_cast<char *>(is_weighted_str.c_str()),
                               metadata_cursor);

    // NUM_NODES = 0
    GraphBase::insert_metadata(node_count,
                               const_cast<char *>(std::to_string(0).c_str()),
                               metadata_cursor);

    // NUM_EDGES = 0
    GraphBase::insert_metadata(edge_count,
                               const_cast<char *>(std::to_string(0).c_str()),
                               metadata_cursor);

    metadata_cursor->close(metadata_cursor);
    session->close(session, NULL);
}

/**
 * @brief This private function inserts metadata values into the metadata
 * table. The fields are self explanatory.
 *
 */
void GraphBase::insert_metadata(const std::string key,
                                const char *value,
                                WT_CURSOR *metadata_cursor)
{
    int ret = 0;
    metadata_cursor->set_key(metadata_cursor, key.c_str());
    metadata_cursor->set_value(metadata_cursor, value);
    if ((ret = metadata_cursor->insert(metadata_cursor)) != 0)
    {
        fprintf(stderr, "failed to insert metadata for key %s", key.c_str());
        // TODO(puneet): Maybe create a GraphException?
    }
}

/**
 * @brief Returns the metadata associated with the key param from the METADATA
 * table.
 */
std::string GraphBase::get_metadata(std::string key, WT_CURSOR *metadata_cursor)
{
    int ret = 0;
    if (metadata_cursor == NULL)
    {
        if ((ret = _get_table_cursor(
                 METADATA, &metadata_cursor, session, false)) != 0)
        {
            fprintf(stderr, "Failed to create cursor to the metadata table.");
            exit(-1);
        }
    }
    metadata_cursor->set_key(metadata_cursor, key.c_str());
    ret = metadata_cursor->search(metadata_cursor);
    if (ret != 0)
    {
        fprintf(
            stderr, "Failed to retrieve metadata for the key %s", key.c_str());
        exit(-1);
    }

    const char *value;
    ret = metadata_cursor->get_value(metadata_cursor, &value);

    return std::string(value);
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
int GraphBase::_get_table_cursor(std::string table,
                                 WT_CURSOR **cursor,
                                 WT_SESSION *session,
                                 bool is_random)
{
    std::string table_name = "table:" + table;
    const char *config = NULL;
    if (is_random)
    {
        config = "next_random=true";
    }
    if (int ret = session->open_cursor(
                      session, table_name.c_str(), NULL, config, cursor) != 0)
    {
        fprintf(
            stderr, "Failed to get table cursor to %s\n", table_name.c_str());
        return ret;
    }
    return 0;
}

void GraphBase::close()
{
    CommonUtil::close_connection(
        conn);  // To update to close sessions in new design
}

// Close, restore from DB, create/drop indices
void GraphBase::__restore_from_db(std::string db_name)
{
    int ret = CommonUtil::open_connection(const_cast<char *>(db_name.c_str()),
                                          opts.stat_log,
                                          opts.conn_config,
                                          &conn);
    WT_CURSOR *cursor = nullptr;

    ret = CommonUtil::open_session(conn, &session);
    const char *key, *value;
    ret = _get_table_cursor(METADATA, &cursor, session, false);

    while ((ret = cursor->next(cursor)) == 0)
    {
        ret = cursor->get_key(cursor, &key);
        ret = cursor->get_value(cursor, &value);

        if (strcmp(key, DB_DIR.c_str()) == 0)
        {
            this->opts.db_dir = value;  // CommonUtil::unpack_string_wt(value,
                                        // this->session);
        }
        else if (strcmp(key, DB_NAME.c_str()) == 0)
        {
            this->opts.db_name = value;  // CommonUtil::unpack_string_wt(value,
                                         // this->session);
        }
        // restore nNodes & nEdges
        else if (strcmp(key, READ_OPTIMIZE.c_str()) == 0)
        {
            if (strcmp(value, "true") == 0)
            {
                this->opts.read_optimize = true;
            }
            else
            {
                this->opts.read_optimize = false;
            }
        }
        else if (strcmp(key, IS_DIRECTED.c_str()) == 0)
        {
            if (strcmp(value, "true") == 0)
            {
                this->opts.is_directed = true;
            }
            else
            {
                this->opts.is_directed = false;
            }
        }
        else if (strcmp(key, IS_WEIGHTED.c_str()) == 0)
        {
            if (strcmp(value, "true") == 0)
            {
                this->opts.is_weighted = true;
            }
            else
            {
                this->opts.is_weighted = false;
            }
        }
    }
}

/**
 * @brief Returns the metadata associated with the key param from the
 * METADATA table. Same from standard_graph implementation
 */

std::string GraphBase::get_metadata(const std::string &key)
{
    int ret = 0;
    WT_CURSOR *metadata_cursor = nullptr;
    if ((ret = _get_table_cursor(METADATA, &metadata_cursor, session, false)) !=
        0)
    {
        fprintf(stderr, "Failed to create cursor to the metadata table.");
        exit(-1);
    }

    metadata_cursor->set_key(metadata_cursor, key.c_str());
    ret = metadata_cursor->search(metadata_cursor);
    if (ret != 0)
    {
        fprintf(
            stderr, "Failed to retrieve metadata for the key %s", key.c_str());
        exit(-1);
    }

    const char *value;
    ret = metadata_cursor->get_value(metadata_cursor, &value);

    return std::string(value);
}

/**
 * @brief Generic function to create the indexes on a table
 *
 * @param table_name The name of the table on which the index is to be
 * created.
 * @param idx_name The name of the index
 * @param projection The columns that are to be included in the index. This
 * is in the format "(col1,col2,..)"
 * @param cursor This is the cursor variable that needs to be set.
 * @return 0 if the index could be set
 */
int GraphBase::_get_index_cursor(std::string table_name,
                                 std::string idx_name,
                                 std::string projection,
                                 WT_CURSOR **cursor)
{
    std::string index_name =
        "index:" + table_name + ":" + idx_name + projection;
    if (int ret = session->open_cursor(
                      session, index_name.c_str(), NULL, NULL, cursor) != 0)
    {
        fprintf(stderr,
                "Failed to open the cursor on the index %s on table %s \n",
                index_name.c_str(),
                table_name.c_str());
        return ret;
    }
    return 0;
}

void GraphBase::set_num_nodes(uint64_t numNodes, WT_CURSOR *metadata_cursor)
{
    return insert_metadata(node_count,
                           const_cast<char *>(std::to_string(numNodes).c_str()),
                           metadata_cursor);
}

void GraphBase::set_num_edges(uint64_t numEdges, WT_CURSOR *metadata_cursor)
{
    return insert_metadata(edge_count,
                           const_cast<char *>(std::to_string(numEdges).c_str()),
                           metadata_cursor);
}

uint64_t GraphBase::get_num_nodes()
{
    std::string found = get_metadata(node_count);
    return std::stoi(found);
}

uint64_t GraphBase::get_num_edges()
{
    std::string found = get_metadata(edge_count);
    return std::stoi(found);
}