//
// Created by 张艺文 on 2018/10/31.
//

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <include/rocksdb/status.h>

namespace rocksdb {


        class NVMFlushJob {
            public:

            NVMFlushJob() = default;

            virtual ~NVMFlushJob() = default;


            virtual void Prepare() = 0;

            virtual Status Run() = 0;

            virtual void Cancel() = 0;

        };


        class EmptyFlushJob : public NVMFlushJob{
        public:
            EmptyFlushJob() = default;
            ~EmptyFlushJob() = default;
            void Prepare() override;
            Status Run() override;
            void Cancel() override;
        };

}  // namespace rocksdb

