//
// Created by 张艺文 on 2018/11/8.
//

#include <cstring>
#include <string>
#include "random.h"
#include <time.h>
#include <io.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "test_common.h"
#include "../libpmemobj++/p.hpp"
#include "../libpmemobj++/persistent_ptr.hpp"
#include "../libpmemobj++/transaction.hpp"
#include "../libpmemobj++/pool.hpp"
#include "../libpmemobj++/make_persistent.hpp"

namespace rocksdb{

    class SequentialWriteTest{
    public:
        SequentialWriteTest(const std::string &path);

        ~SequentialWriteTest() =default;

        double Run();

    private:
        void write(const char* data);
        void Print();
        pmem::obj::pool_base pop_;
        pmem::obj::p<uint64_t> count_;
        pmem::obj::persistent_ptr<std::string> data_[10000];

    };

    SequentialWriteTest::SequentialWriteTest(const std::string &path) {
        if(file_exists(path.c_str())){
            pop_ = pmem::obj::pool<SequentialWriteTest>::create(path.c_str(), "layout", PMEMOBJ_MIN_POOL, CREATE_MODE_RW);
        }else{
            pop_ = pmem::obj::pool<SequentialWriteTest>::open(path.c_str(), "layout");
        }

        count_ = 0;
    }

    void SequentialWriteTest::write(const char *data) {
        pmem::obj::transaction::run(pop_, [&]{
            data_[count_] = pmem::obj::make_persistent<std::string>(data);
            count_ = count_ + 1;
        });
    }

    void SequentialWriteTest::Print() {
        for(int i = 0; i < 10000; i++){
            std::string tmp(data_[i]->c_str());
        }
    }


    double SequentialWriteTest::Run() {
        auto rnd = rocksdb::Random::GetTLSInstance();
        time_t startt, endt;
        startt = clock();
        for(int i = 0; i < 1000 ; i++){
            auto num = rnd->Next();
            char buf[16];
            sprintf(buf, "%15d", num);
            write(buf);
        }
        endt = clock();
        printf("cost  = %f\n", (double)(endt - startt) / CLOCKS_PER_SEC);

        startt = clock();
        Print();
        endt = clock();
        printf("cost  = %f\n", (double)(endt - startt) / CLOCKS_PER_SEC);
    }

    class SSDWriteTest{
    public:
        SSDWriteTest(const std::string &path);

        ~SSDWriteTest() =default;

        double Run();

    private:
        void Write(const std::string& data);
        void Print();
        int fd_;
        uint64_t offset_;

    };

    SSDWriteTest::SSDWriteTest(const std::string &path) {
        fd_ = open(path.c_str(), O_CREAT | O_RW);

        offset_ = 0;
    }

    void SSDWriteTest::Write(const std::string& data) {
        write(fd_, data.c_str(), data.size());
        offset_ += data.size();
    }

    void SSDWriteTest::Print() {

    }


    double SSDWriteTest::Run() {
        auto rnd = rocksdb::Random::GetTLSInstance();
        time_t startt, endt;
        startt = clock();
        for(int i = 0; i < 10000 ; i++){
            auto num = rnd->Next();
            char buf[1024];
            sprintf(buf, "%1023d", num);
            Write(buf);
        }
        endt = clock();
        printf("cost  = %f\n", (double)(endt - startt) / CLOCKS_PER_SEC);

        startt = clock();
        Print();
        endt = clock();
        printf("cost  = %f\n", (double)(endt - startt) / CLOCKS_PER_SEC);
    }



}//end rocksdb


int main(){
    rocksdb::SSDWriteTest *test = new rocksdb::SSDWriteTest("/tmp/testfile");
    test->Run();
    delete test;
}


