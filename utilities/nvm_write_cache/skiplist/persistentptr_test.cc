//
// Created by 张艺文 on 2018/11/20.
//
#include <cstring>
#include "test_common.h"
#include "libpmemobj++/persistent_ptr.hpp"
#include "libpmemobj++/transaction.hpp"
#include "libpmemobj++/make_persistent.hpp"
#include "libpmemobj++/make_persistent_array.hpp"

using namespace pmem::obj;
struct root{
    persistent_ptr<char[]> persistent_buffer;
};
int main() {
    pool<root> pop;
    std::string path("/pmem/ptr_test");
    if (file_exists(path.c_str()) != 0) {
        pop = pool<root>::create(path.c_str(), "layout", uint64_t(1024), CREATE_MODE_RW);
    } else {
        pop = pool<root>::open(path.c_str(), "layout");
    }

    persistent_ptr<root> rootp = pop.root();

    if(rootp->persistent_buffer != nullptr){
        transaction::run(pop, [&]{
            rootp->persistent_buffer = make_persistent<char[]>(512);
        });
        char* raw_buffer = rootp->persistent_buffer.get();
        std::string test("hello world");
        memcpy(raw_buffer, test.c_str(), test.size());
    } else{
        std::string tmp(rootp->persistent_buffer.get());
        printf("%s\n", tmp.c_str());
    }
    return 0;
}