//
// Created by 张艺文 on 2018/11/2.
//

#include <inttypes.h>

#include <monitoring/thread_status_util.h>
#include <table/merging_iterator.h>
#include <db/snapshot_checker.h>
#include <db/compaction_iterator.h>
#include <db/event_helpers.h>
#include "nvm_write_cache.h"
#include "db/column_family.h"
#include "util/log_buffer.h"
#include "util/event_logger.h"
#include "db/job_context.h"
#include "db/memtable.h"
#include "include/rocksdb/table.h"

#include "nvm_cache_options.h"
#include "prefix_extractor.h"
#include "fixed_range_based_flush_job.h"
#include "fixed_range_chunk_based_nvm_write_cache.h"
#include "chunk.h"
#include "fixed_range_tab.h"


namespace rocksdb {

const char *NVMGetFlushReasonString(FlushReason flush_reason) {
    switch (flush_reason) {
        case FlushReason::kOthers:
            return "Other Reasons";
        case FlushReason::kGetLiveFiles:
            return "Get Live Files";
        case FlushReason::kShutDown:
            return "Shut down";
        case FlushReason::kExternalFileIngestion:
            return "External File Ingestion";
        case FlushReason::kManualCompaction:
            return "Manual Compaction";
        case FlushReason::kWriteBufferManager:
            return "Write Buffer Manager";
        case FlushReason::kWriteBufferFull:
            return "Write Buffer Full";
        case FlushReason::kTest:
            return "Test";
        case FlushReason::kDeleteFiles:
            return "Delete Files";
        case FlushReason::kAutoCompaction:
            return "Auto Compaction";
        case FlushReason::kManualFlush:
            return "Manual Flush";
        case FlushReason::kErrorRecovery:
            return "Error Recovery";
        default:
            return "Invalid";
    }
}


FixedRangeBasedFlushJob::FixedRangeBasedFlushJob(const std::string &dbname,
                                                 const ImmutableDBOptions &db_options,
                                                 JobContext *job_context,
                                                 EventLogger *event_logger,
                                                 rocksdb::ColumnFamilyData *cfd,
                                                 std::vector<SequenceNumber> existing_snapshots,
                                                 SequenceNumber earliest_write_conflict_snapshot,
                                                 SnapshotChecker *snapshot_checker,
                                                 rocksdb::InstrumentedMutex *db_mutex,
                                                 std::atomic<bool> *shutting_down,
                                                 LogBuffer *log_buffer,
                                                 Statistics* stats,
                                                 rocksdb::NVMCacheOptions *nvm_cache_options)
        : dbname_(dbname),
          db_options_(db_options),
          job_context_(job_context),
          event_logger_(event_logger),
          cfd_(cfd),
          existing_snapshots_(std::move(existing_snapshots)),
          earliest_write_conflict_snapshot_(earliest_write_conflict_snapshot),
          snapshot_checker_(snapshot_checker),
          db_mutex_(db_mutex),
          shutting_down_(shutting_down),
          log_buffer_(log_buffer),
          stats_(stats),
          nvm_cache_options_(nvm_cache_options),
          nvm_write_cache_(dynamic_cast<FixedRangeChunkBasedNVMWriteCache *>(nvm_cache_options_->nvm_write_cache_)),
          //range_list_(nvm_write_cache_->GetRangeList()),
          last_chunk(nullptr) {

}


FixedRangeBasedFlushJob::~FixedRangeBasedFlushJob() {

}

void FixedRangeBasedFlushJob::Prepare() {
    db_mutex_->AssertHeld();

    // pick memtables from immutable memtable list
    cfd_->imm()->PickMemtablesToFlush(nullptr, &mems_);
    if (mems_.empty()) return;

    //Report flush inpyt size
    ReportFlushInputSize(mems_);
}


Status FixedRangeBasedFlushJob::Run() {
    db_mutex_->AssertHeld();
    AutoThreadOperationStageUpdater stage_run(
            ThreadStatus::STAGE_FLUSH_RUN);
    if (mems_.empty()) {
        ROCKS_LOG_BUFFER(log_buffer_, "[%s] Nothing in memtable to flush",
                         cfd_->GetName().c_str());
        return Status::OK();
    }

    Status s = InsertToNVMCache();

    if (s.ok() &&
        (shutting_down_->load(std::memory_order_acquire) || cfd_->IsDropped())) {
        s = Status::ShutdownInProgress(
                "Database shutdown or Column family drop during flush");
    }

    if (!s.ok()) {
        cfd_->imm()->RollbackMemtableFlush(mems_, 0);
    } else {
        // record this flush to manifest or not?
    }

    return s;

}

Status FixedRangeBasedFlushJob::InsertToNVMCache() {
    AutoThreadOperationStageUpdater stage_updater(
            ThreadStatus::STAGE_FLUSH_WRITE_L0);
    db_mutex_->AssertHeld();
    const uint64_t start_micros = db_options_.env->NowMicros();
    Status s;
    {
        // unlock
        db_mutex_->Unlock();
        if (log_buffer_) {
            log_buffer_->FlushBufferToLog();
        }
        std::vector<InternalIterator *> memtables;
        std::vector<InternalIterator *> range_del_iters;
        ReadOptions ro;
        ro.total_order_seek = true;
        Arena arena;
        uint64_t total_num_entries = 0, total_num_deletes = 0;
        size_t total_memory_usage = 0;
        for (MemTable *m : mems_) {
            /*  ROCKS_LOG_INFO(
                      db_options_.info_log,
                      "[%s] [JOB %d] Flushing memtable with next log file: %"
                              PRIu64
                              "\n",
                      cfd_->GetName().c_str(), job_context_->job_id, m->GetNextLogNumber());*/
            memtables.push_back(m->NewIterator(ro, &arena));
            auto *range_del_iter = m->NewRangeTombstoneIterator(ro);
            if (range_del_iter != nullptr) {
                range_del_iters.push_back(range_del_iter);
            }
            total_num_entries += m->num_entries();
            total_num_deletes += m->num_deletes();
            total_memory_usage += m->ApproximateMemoryUsage();
        }
        event_logger_->Log()
                << "job" << job_context_->job_id << "event"
                << "flush_started"
                << "num_memtables" << mems_.size() << "num_entries" << total_num_entries
                << "num_deletes" << total_num_deletes << "memory_usage"
                << total_memory_usage << "flush_reason"
                << NVMGetFlushReasonString(cfd_->GetFlushReason());

        {
            ScopedArenaIterator iter(
                    NewMergingIterator(&cfd_->internal_comparator(), &memtables[0],
                                       static_cast<int>(memtables.size()), &arena));
            std::unique_ptr<InternalIterator> range_del_iter(NewMergingIterator(
                    &cfd_->internal_comparator(),
                    range_del_iters.empty() ? nullptr : &range_del_iters[0],
                    static_cast<int>(range_del_iters.size())));
            ROCKS_LOG_INFO(db_options_.info_log,
                           "[%s] [JOB %d] NVM cache flush: started",
                           cfd_->GetName().c_str(), job_context_->job_id);

            int64_t _current_time = 0;
            auto status = db_options_.env->GetCurrentTime(&_current_time);
            // Safe to proceed even if GetCurrentTime fails. So, log and proceed.
            if (!status.ok()) {
                ROCKS_LOG_WARN(
                        db_options_.info_log,
                        "Failed to get current time to populate creation_time property. "
                        "Status: %s",
                        status.ToString().c_str());
            }
            /*const uint64_t current_time = static_cast<uint64_t>(_current_time);*/

            /*uint64_t oldest_key_time =
                    mems_.front()->ApproximateOldestKeyTime();*/

            s = BuildChunkAndInsert(iter.get(),
                                    std::move(range_del_iter),
                                    cfd_->internal_comparator(),
                                    std::move(existing_snapshots_),
                                    earliest_write_conflict_snapshot_,
                                    snapshot_checker_,
                                    event_logger_, job_context_->job_id);

            LogFlush(db_options_.info_log);
        }
        db_mutex_->Lock();
    }
    InternalStats::CompactionStats stats(CompactionReason::kFlush, 1);
    stats.micros = db_options_.env->NowMicros() - start_micros;
    MeasureTime(stats_, FLUSH_TIME, stats.micros);
    return s;
}

Status FixedRangeBasedFlushJob::BuildChunkAndInsert(InternalIterator *iter,
                                                    std::unique_ptr<InternalIterator> range_del_iter,
                                                    const InternalKeyComparator &internal_comparator,
                                                    std::vector<SequenceNumber> snapshots,
                                                    SequenceNumber earliest_write_conflict_snapshot,
                                                    SnapshotChecker *snapshot_checker,
                                                    EventLogger *event_logger, int job_id) {
    Status s;
    // Internal Iterator
    iter->SeekToFirst();
    std::unique_ptr<RangeDelAggregator> range_del_agg(
            new RangeDelAggregator(internal_comparator, snapshots));
    s = range_del_agg->AddTombstones(std::move(range_del_iter));
    if (!s.ok()) {
        // may be non-ok if a range tombstone key is unparsable
        return s;
    }


    if (iter->Valid() || !range_del_agg->IsEmpty()) {
        PrefixExtractor *prefix_extractor = nvm_write_cache_->internal_options()->prefix_extractor_;


        MergeHelper merge(db_options_.env, internal_comparator.user_comparator(),
                          cfd_->ioptions()->merge_operator, nullptr, cfd_->ioptions()->info_log,
                          true /* internal key corruption is not ok */,
                          snapshots.empty() ? 0 : snapshots.back(),
                          snapshot_checker);

        CompactionIterator c_iter(
                iter, internal_comparator.user_comparator(), &merge, kMaxSequenceNumber,
                &snapshots, earliest_write_conflict_snapshot, snapshot_checker, db_options_.env,
                ShouldReportDetailedTime(db_options_.env, cfd_->ioptions()->statistics),
                true /* internal key corruption is not ok */, range_del_agg.get());
        c_iter.SeekToFirst();

        for (; c_iter.Valid(); c_iter.Next()) {
            const Slice &key = c_iter.key();
            const Slice &value = c_iter.value();
            //printf("get key %s\n", key.data_);

            std::string now_prefix = (*prefix_extractor)(key.data_, key.size_);
            if (now_prefix == last_prefix && last_chunk != nullptr) {
                last_chunk->Insert(key, value);
            } else {
                /*auto range_found = range_list_->find(now_prefix);
                if (range_found == range_list_->end()) {
                    // this is a new prefix, biuld a new range
                    // new a range mem and update range list
                    nvm_write_cache_->NewRange(now_prefix);
                }*/
                BuildingChunk *now_chunk = nullptr;
                auto chunk_found = pending_output_chunk.find(now_prefix);
                if (chunk_found == pending_output_chunk.end()) {
                    //this is a new build a new chunk
                    auto new_chunk = new BuildingChunk(
                            nvm_write_cache_->internal_options()->filter_policy_,
                            now_prefix);
                    pending_output_chunk[now_prefix] = new_chunk;
                    now_chunk = new_chunk;
                } else {
                    now_chunk = chunk_found->second;
                }
                // add data to this chunk
                now_chunk->Insert(key, value);
                last_chunk = now_chunk;
                last_prefix = now_prefix;

            }
        }
        s = c_iter.status();
        if (s.ok()) {
            // check is the prefix existing in nvm cache or create it
            for(auto pendding_chunk:pending_output_chunk){
                nvm_write_cache_->RangeExistsOrCreat(pendding_chunk.first);
            }
            // insert data of each range into nvm cache
            std::vector<port::Thread> thread_pool;
            thread_pool.clear();
            auto finish_build_chunk = [&](std::string prefix) {
                // get chunk data
                char* bloom_data;
                ChunkMeta meta;
                meta.prefix = prefix;
                std::string *output_data = pending_output_chunk[prefix]->Finish(&bloom_data,
                        meta.cur_start, meta.cur_end);
                // append to range tab
                //range_found->second.Append(bloom_data/*char**/, output_data/*Slice*/,ChunkMeta(internal_comparator, cur_start, cur_end));
                nvm_write_cache_->AppendToRange(internal_comparator, bloom_data, output_data->c_str(), meta);
                // TODO:Slice是否需要delete
                delete output_data;
                delete[] bloom_data;
            };

            auto pending_chunk = pending_output_chunk.begin();
            pending_chunk++;
            for (; pending_chunk != pending_output_chunk.end(); pending_chunk++) {
                thread_pool.emplace_back(finish_build_chunk, pending_chunk->first);
            }
            finish_build_chunk(pending_output_chunk.begin()->first);
            for (auto &running_thread : thread_pool) {
                running_thread.join();
            }
            // check if there is need for compaction
            nvm_write_cache_->MaybeNeedCompaction();

        } else {
            return s;
        };

        // not supprt range del currently
        /*for (auto it = range_del_agg->NewIterator(); it->Valid(); it->Next()) {
            auto tombstone = it->Tombstone();
            auto kv = tombstone.Serialize();
            builder->Add(kv.first.Encode(), kv.second);
            *//*meta->UpdateBoundariesForRange(kv.first, tombstone.SerializeEndKey(),
                                               tombstone.seq_, internal_comparator);*//*
            }*/

        // Finish and check for file errors
        if (s.ok()) {
            StopWatch sw(db_options_.env, cfd_->ioptions()->statistics, TABLE_SYNC_MICROS);
        }
    }
    return s;
}


void FixedRangeBasedFlushJob::Cancel() {

}

void FixedRangeBasedFlushJob::ReportFlushInputSize(const rocksdb::autovector<rocksdb::MemTable *> &mems) {
    uint64_t input_size = 0;
    for (auto *mem : mems) {
        input_size += mem->ApproximateMemoryUsage();
    }
    ThreadStatusUtil::IncreaseThreadOperationProperty(
            ThreadStatus::FLUSH_BYTES_MEMTABLES,
            input_size);
}

}//end rocksdb