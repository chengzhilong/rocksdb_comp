#include "db/fixed_range_based_compaction_job.h"

#include <inttypes.h>
#include <algorithm>
#include <functional>
#include <list>
#include <memory>
#include <random>
#include <set>
#include <thread>
#include <utility>
#include <vector>

#include "db/builder.h"
#include "db/db_impl.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/error_handler.h"
#include "db/event_helpers.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/memtable_list.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/version_set.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/block.h"
#include "table/block_based_table_factory.h"
#include "table/merging_iterator.h"
#include "table/table_builder.h"
#include "util/coding.h"
#include "util/file_reader_writer.h"
#include "util/filename.h"
#include "util/log_buffer.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/sst_file_manager_impl.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/sync_point.h"

namespace rocksdb {

const char* GetFixedRangeCompactionReasonString(CompactionReason compaction_reason) {
    switch (compaction_reason) {
        case CompactionReason::kUnknown:
            return "Unknown";
        case CompactionReason::kLevelL0FilesNum:
            return "LevelL0FilesNum";
        case CompactionReason::kFilesMarkedForCompaction:
            return "FilesMarkedForCompaction";
        case CompactionReason::kBottommostFiles:
            return "BottommostFiles";
        case CompactionReason::kLevelMaxLevelSize:
            return "LevelMaxLeelSize";
        case CompactionReason::kUniversalSizeAmplification:
            return "UniversalSizeAmplification";
        case CompactionReason::kUnversalSortedRunNum:
            return "UniversalSortedRunNum";
        case CompactionReason::kManualCompaction:
            return "ManualCompaction";
        case CompactionReason::kNumOfReasons:
            // fall through
        default:
            assert(false);
            return "Invalid";
    }
}

struct FixedRangeBasedCompactionJob::SubCompactionState {
    const Compaction* compaction;
    std::unique_ptr<CompactionIterator> c_iter;

    Slice *start, *end;
    Status status;

    Struct Output {
        FileMetaData meta;
        bool finished;
        std::shared_ptr<const TableProperties> table_properties;
    };

    // State kept for output being generated
    std::vector<Output> outputs;
    std::unique_ptr<WritableFileWriter> outfile;
    std::unique_ptr<TableBuilder> builder;
    Output* current_output() {
        if (outputs.empty()) {
            // This subcompaction's outptut could be empty if compaction was aborted
            // before this subcompaction had a chance to generate any output files.
            // When subcompactions are executed sequentially this is more likely and
            // will be particulalry likely for the later subcompactions to be empty.
            // Once they are run in parallel however it should be much rarer.
            return nullptr;
        } else {
            return &outputs.back();
        }
    }

    uint64_t current_output_file_size;

    // State during the subcompaction
    uint64_t total_bytes;
    uint64_t num_input_records;
    uint64_t num_output_records;
    CompactionJobStats compaction_job_stats;
    uint64_t approx_size;
    // An index that used to speed up ShouldStopBefore().
    size_t grandparent_index = 0;
    // The number of bytes overlapping between the current output and
    // grandparent files used in ShouldStopBefore().
    uint64_t overlapped_bytes = 0;
    // A flag determine whether the key has been seen in ShouldStopBefore()
    bool seen_key = false;
    std::string compression_dict;

    SubcompactionState(Compaction* c, Slice* _start, Slice* _end,
    uint64_t size = 0)
    : compaction(c),
            start(_start),
            end(_end),
            outfile(nullptr),
    builder(nullptr),
    current_output_file_size(0),
    total_bytes(0),
    num_input_records(0),
    num_output_records(0),
    approx_size(size),
            grandparent_index(0),
    overlapped_bytes(0),
    seen_key(false),
    compression_dict() {
        assert(compaction != nullptr);
    }

    SubcompactionState(SubcompactionState&& o) { *this = std::move(o); }

    SubcompactionState& operator=(SubcompactionState&& o) {
        compaction = std::move(o.compaction);
        start = std::move(o.start);
        end = std::move(o.end);
        status = std::move(o.status);
        outputs = std::move(o.outputs);
        outfile = std::move(o.outfile);
        builder = std::move(o.builder);
        current_output_file_size = std::move(o.current_output_file_size);
        total_bytes = std::move(o.total_bytes);
        num_input_records = std::move(o.num_input_records);
        num_output_records = std::move(o.num_output_records);
        compaction_job_stats = std::move(o.compaction_job_stats);
        approx_size = std::move(o.approx_size);
        grandparent_index = std::move(o.grandparent_index);
        overlapped_bytes = std::move(o.overlapped_bytes);
        seen_key = std::move(o.seen_key);
        compression_dict = std::move(o.compression_dict);
        return *this;
    }

    // Because member unique_ptrs do not have these.
    SubcompactionState(const SubcompactionState&) = delete;

    SubcompactionState& operator=(const SubcompactionState&) = delete;

    // Returns true iff we should stop building the current output
    // before processing "internal_key".
    bool ShouldStopBefore(const Slice& internal_key, uint64_t curr_file_size) {
        const InternalKeyComparator* icmp =
                &compaction->column_family_data()->internal_comparator();
        const std::vector<FileMetaData*>& grandparents = compaction->grandparents();

        // Scan to find earliest grandparent file that contains key.
        while (grandparent_index < grandparents.size() &&
               icmp->Compare(internal_key,
                             grandparents[grandparent_index]->largest.Encode()) >
               0) {
            if (seen_key) {
                overlapped_bytes += grandparents[grandparent_index]->fd.GetFileSize();
            }
            assert(grandparent_index + 1 >= grandparents.size() ||
                   icmp->Compare(
                           grandparents[grandparent_index]->largest.Encode(),
                           grandparents[grandparent_index + 1]->smallest.Encode()) <= 0);
            grandparent_index++;
        }
        seen_key = true;

        if (overlapped_bytes + curr_file_size >
            compaction->max_compaction_bytes()) {
            // Too much overlap for current output; start new output
            overlapped_bytes = 0;
            return true;
        }

        return false;
    }
};

void FixedRangeBasedCompaction::Prepare()



};

}