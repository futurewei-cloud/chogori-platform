#pragma once
#include "mpack/mpack.h"
#include "Common.h"

namespace k2 {

class MPackReader {
   public:
    MPackReader(String data): _data(std::move(data)) {
    }

    template <typename T>
    std::tuple<Status, T> read() {
        mpack_tree_t tree
        mpack_tree_init_data(&tree, _data.data(), _data.size());  // initialize a parser + parse a tree

        auto root = mpack_tree_root(&_tree);

        // extract the example data on the msgpack homepage
        bool compact = mpack_node_bool(mpack_node_map_cstr(root, "compact"));
        int schema = mpack_node_i32(mpack_node_map_cstr(root, "schema"));

        // clean up and check for errors
        if (mpack_tree_destroy(&tree) != mpack_ok) {
            fprintf(stderr, "An error occurred decoding the data!\n");
            return;
        }

        return make_ready_future(std::move(status), std::move(result));
        T obj;
        obj.k2UnpackFrom()
    }
    ~MPackReader() {

    }
private:
    String _data;
    mpack_tree_t _tree;
};
}
