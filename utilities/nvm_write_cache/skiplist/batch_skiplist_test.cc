//
// Created by 张艺文 on 2018/11/15.
//

#include "persistent_batchupdate_skiplist.h"
#include "test_common.h"
#include <string>
#include <time.h>
#include "util/random.h"


int main(int argc, char* argv[]){
    std::string path(argv[1]);
    pool<rocksdb::SkiplistWrapper> pop;
    if (file_exists(path.c_str()) != 0) {
        pop = pool<rocksdb::SkiplistWrapper>::create(path.c_str(), "layout", uint64_t(4ul*1024*1024*1024), CREATE_MODE_RW);
    } else {
        pop = pool<rocksdb::SkiplistWrapper>::open(path.c_str(), "layout");
    }

    persistent_ptr<rocksdb::SkiplistWrapper> skiplist = pop.root();

    skiplist->Init(pop, 12, 4);

    auto rnd = rocksdb::Random::GetTLSInstance();
    time_t startt, endt;
    startt = clock();
    int count = 0;
    for(int i = 0; i < 10000 ; i++){
        auto number = rnd->Next();
        char buf[4096];
        sprintf(buf, "%4095d", number);
        skiplist->batchskiplist->Insert(buf);
        if(count++ == 100){
            printf("finished %d\n", i);
            count = 0;
        }
    }
    endt = clock();
    printf("insert cost  = %f\n", (double)(endt - startt) / CLOCKS_PER_SEC);
    //startt = clock();
    //skiplist->Print();
    //endt = clock();
    //printf("print cost  = %f\n", (double)(endt - startt) / CLOCKS_PER_SEC);

    pop.close();

    return 0;
}
