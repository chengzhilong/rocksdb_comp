//
// Created by 张艺文 on 2018/10/31.
//

#include "nvm_flush_job.h"

namespace rocksdb{



    Status EmptyFlushJob::Run() {
        return Status::OK();
    }

    void EmptyFlushJob::Prepare() {

    }

    void EmptyFlushJob::Cancel() {

    }

}//end rocksdb