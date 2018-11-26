//
// Created by 张艺文 on 2018/11/2.
//

#pragma once


#include <string>
#include <util/autovector.h>
#include <atomic>
#include <monitoring/instrumented_mutex.h>
#include <table/internal_iterator.h>

#include "utilities/nvm_write_cache/nvm_write_cache.h"
#include "utilities/nvm_write_cache/nvm_flush_job.h"

namespace rocksdb {

class ColumnFamilyData;

class MemTable;

struct NVMCacheOptions;
struct FixedRangeChunkBasedCacheStats;

class FixedRangeChunkBasedNVMWriteCache;

class LogBuffer;

struct JobContext;

class EventLogger;

class SnapshotChecker;

class RangeBasedChunk;

class BuildingChunk;

class FixedRangeTab;

class FixedRangeBasedFlushJob : public NVMFlushJob {
public:

    explicit FixedRangeBasedFlushJob(
            const std::string &dbname,
            const ImmutableDBOptions &db_options,
            JobContext *job_context,
            EventLogger *event_logger,
            ColumnFamilyData *cfd,
            std::vector<SequenceNumber> existing_snapshots,
            SequenceNumber earliest_write_conflict_snapshot,
            SnapshotChecker *snapshot_checker,
            InstrumentedMutex *db_mutex,
            std::atomic<bool> *shutting_down,
            LogBuffer *log_buffer,
            Statistics* stats,
            NVMCacheOptions *nvm_cache_options);

    ~FixedRangeBasedFlushJob() override;

    void Prepare() override;

    Status Run() override;

    void Cancel() override;

private:

    void ReportFlushInputSize(const autovector<MemTable *> &mems);

    Status InsertToNVMCache();

    Status BuildChunkAndInsert(InternalIterator *iter,
                               std::unique_ptr<InternalIterator> range_del_iter,
                               const InternalKeyComparator &internal_comparator,
                               std::vector<SequenceNumber> snapshots,
                               SequenceNumber earliest_write_conflict_snapshot,
                               SnapshotChecker *snapshot_checker,
                               EventLogger *event_logger, int job_id);

    const std::string &dbname_;
    const ImmutableDBOptions &db_options_;
    JobContext *job_context_;
    EventLogger *event_logger_;
    ColumnFamilyData *cfd_;

    std::vector<SequenceNumber> existing_snapshots_;
    SequenceNumber earliest_write_conflict_snapshot_;
    SnapshotChecker *snapshot_checker_;

    InstrumentedMutex *db_mutex_;
    std::atomic<bool> *shutting_down_;
    LogBuffer *log_buffer_;
    Statistics* stats_;

    const NVMCacheOptions *nvm_cache_options_;
    FixedRangeChunkBasedNVMWriteCache *nvm_write_cache_;
    //FixedRangeChunkBasedCacheStats* cache_stat_;
    //std::unordered_map<std::string, FixedRangeTab> *range_list_;

    autovector<MemTable *> mems_;

    std::unordered_map<std::string, BuildingChunk *> pending_output_chunk;

    std::string last_prefix;

    BuildingChunk *last_chunk;


};

}//end rocksdb
