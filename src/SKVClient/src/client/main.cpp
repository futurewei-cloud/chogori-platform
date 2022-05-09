#include "mpack.h"

#include <string>
#include <iostream>

std::string buildMap(bool flag, int val) {
    char* data;
    size_t size;
    mpack_writer_t writer;
    mpack_writer_init_growable(&writer, &data, &size);

    // write the example on the msgpack homepage
    mpack_build_map(&writer);
    mpack_write_cstr(&writer, "compact");
    mpack_write_bool(&writer, flag);
    mpack_write_cstr(&writer, "val");
    mpack_write_uint(&writer, val);
    mpack_complete_map(&writer);

    // finish writing
    if (mpack_writer_destroy(&writer) != mpack_ok) {
        std::cerr << "An error occurred encoding the data!" << std::endl;
        return std::string{};
    }

    // use the data
    std::string res(data, size);
    free(data);
    return res;
}

struct MReader {
    MReader(std::string buf): _buf(buf){
        mpack_tree_init_data(&_tree, _buf.data(), _buf.size());
    }
private:
    std::string _buf;
    mpack_tree_t _tree;
};
int main() {
    auto s1 = buildMap(true, 10);
    auto s2 = buildMap(false, 20);
    auto buf = s1 + s2;
    // parse a file into a node tree
    mpack_tree_t tree;
    mpack_tree_init_data(&tree, buf.data(), buf.size());

    {
        mpack_tree_parse(&tree);
        mpack_node_t root = mpack_tree_root(&tree);
        std::cerr << " Root 1 is: " << mpack_node_is_nil(root) << std::endl;
    }
    {
        mpack_tree_parse(&tree);
        mpack_node_t root = mpack_tree_root(&tree);
        std::cerr << " Root 2 is: " << mpack_node_is_nil(root) << std::endl;
    }

    // clean up and check for errors
    if (mpack_tree_destroy(&tree) != mpack_ok) {
        std::cerr << "An error occurred decoding the data!" << std::endl;
        return 0;
    }
    return 0;
}
