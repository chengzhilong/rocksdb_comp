//
// Created by 程志龙 on 2018/11/12
//

#pragma once

#include <string>
#include <deque>
#include <limits>
#include <set>

#include "db/column_family.h"
#include "db/compaction_iterator.h"
#include "db/dbformat.h"
#include "db/internal_stats.h"
#include "db/job_context.h"
#include "db/log_writer.h"
#include "db/version_edit.h"
#include "db/write_controller.h"
#include "db/write_thread.h"
#include "options/db_options.h"
#include "options/cf_options.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "util/autovector.h"
#include "util/event_logger.h"
#include "util/thread_local.h"

namespace rocksdb {

class Arena;
class Version;
class Version;
class VersionEdit;
class VersionSet;
class SnapshotChecker;

class FixedRangeBasedCompactionJob {
public:
    FixedRangeBasedCompactionJob(

    );

    ~FixedRangeBasedCompactionJob();

    FixedRangeBasedCompactionJob(FixedRangeBasedCompactionJob&& job) = delete;
    FixedRangeBasedCompactionJob(const FixedRangeBasedCompactionJob& job) = delete;
    FixedRangeBasedCompactionJob& operator=(const FixedRangeBasedCompactionJob& job) = delete;

    // REQUIRED: mutex held
    void Prepare();
    // REQUIRED: mutex not held
    Status Run();

    // REQUIRED: mutex held
    Status Install(const MutableCFOptions& mutable_cf_options);

private:
    struct SubcompactionState;
    void ReportStartedCompaction(Compaction* compaction);

    void  ProcessKeyValueCompaction(SubcompactionState* sub_compact);
    Status FinishCompactionOutputFile(const Status& input_status, SubcompactionState* sub_compact,
        RangeDelAggregator* range_del_agg, CompactionIteartionStats* range_del_out_stats,
        const Slice* next_table_min_key = nullptr);
    Status InstallCompactionResults(const MutableCFOptions& mutable_cf_options);
    Status OpenCompactionOutputFile(SubcompactionState* sub_compact);
    void CleanupCompaction();

    void UpdateCompactionJobStats(const InternalStats::CompactionStats& stats) const;
    void LogCompaction();

    int job_id;
    
    // CompactionJob state
    struct CompactionState;
    CompactionState* compact_;
    CompactionJobStats* compaction_job_stats_;

    // DBImpl state
    const std::string& db_name;
    const ImmutableDBOptions& db_options_;
    const EnvOptions env_options_;

    Env* env_;
    VersionSet* versions_;
    const std::atomic<bool>* shutting_down_;
    const SequenceNumber preserve_deletes_seqnum_;
    LogBuffer* log_buffer_;
    Directory* output_directory_;
    InstrumentedMutex* db_mutex_;
    ErrorHandler* db_error_handler_;
    std::vector<SequenceNumber> existing_snapshots_;
    
    std::shared_ptr<Cache> table_cache_;
    EventLogger* event_logger_;
    bool bottommost_level_;

    std::vector<Slice> boundaries_;
    std::vector<uint64_t> sizes_;
};

}                   // namespace rocksdb