/*
 * Copyright 2022 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "cukeydist.hpp"

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/scalar.h>
#include <parquet/arrow/schema.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/statistics.h>
#include <parquet/types.h>

#include <algorithm>
#include <chrono>
#include <csignal>
#include <cudf/concatenate.hpp>
#include <cudf/copying.hpp>
#include <cudf/interop.hpp>
#include <cudf/io/datasource.hpp>
#include <cudf/io/parquet.hpp>
#include <cudf/io/types.hpp>
#include <cudf/merge.hpp>
#include <cudf/sorting.hpp>
#include <cudf/table/table.hpp>
#include <cudf/types.hpp>  //cudf size_type
#include <cudf/utilities/bit.hpp>
#include <cudf/utilities/error.hpp>
#include <exception>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <rmm/mr/device/logging_resource_adaptor.hpp>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "cukeydist_types.hpp"
#include "cukeydist_utils.hpp"
#include "filters.h"
#include "histo.hpp"
#include "msgpack/msgpack.hpp"
#include "output_split.hpp"
#include "s3_utils.hpp"
#include "s3_waiting_source.hpp"
#include "s3_writer.hpp"

template <typename T>
std::pair<MergeResult, std::unique_ptr<cudf::table>>
load_sort(cudf::io::source_info const& si,
          std::vector<std::vector<cudf::size_type>> const& row_groups,
          T const& low, T const& high,
          cudf::size_type sort_col) {
    try {
        auto builder = cudf::io::parquet_reader_options::builder(si).row_groups(row_groups);
        auto opts = builder.build();
        auto table_with_metadata = cudf::io::read_parquet(opts);
        std::unique_ptr<cudf::table> filtered = filter_table_by_range(table_with_metadata.tbl->view(),
                                                                      table_with_metadata.tbl->view().column(sort_col),
                                                                      low, high);
        // fail if the column is not already sorted
        if (!cudf::is_sorted(filtered->view().select({sort_col}), {cudf::order::ASCENDING}, {cudf::null_order::BEFORE})) {
            std::cerr << "File is not in sorted order!\n";
            std::abort();
        }

        if (filtered->num_rows() == 0)
            return {MergeResult::Success, nullptr};
        // this must be here since we load a bunch of sorted row groups into one table
        // this should be replaced with a straight merge
        // auto sorted = cudf::sort_by_key(filtered->view(), filtered->view().select({sort_col}));
        return {MergeResult::Success, std::move(filtered)};
    } catch (std::bad_alloc const& e) {
        std::cerr << "Caught error " << e.what() << std::endl;
        return {MergeResult::AllocFail, nullptr};
    }
}

// hack: global singleton!
static inline bool usesS3Input = false;

template <typename T>
tab_mem_t
table_for_range_low_mem(std::vector<fileinfo_t> const& sources, cudf::size_type const sort_col,
                        T const& low, T const& high,
                        std::vector<EdgePair_vec<T>> const& filestats,
                        std::shared_ptr<std::atomic_size_t> downloadedAmount) {
    freeMemory("build tables start");
    ::size_t memUsed = 0;
    std::vector<std::unique_ptr<cudf::table>> sortedTables;
    std::vector<cudf::size_type> rgs;
    std::vector<std::string> input_files;
    std::vector<std::vector<cudf::size_type>> row_groups;

    for (::size_t j = 0; j < sources.size(); j++) {
        fileinfo_t const& source = sources[j];
        EdgePair_vec<T> const& stats = filestats[source.stIndex];
        EdgePair<T> const& s = stats[static_cast<::size_t>(source.rgidx)];

        if (low <= s.second && high >= s.first) {
            // first one
            if (input_files.size() == 0) {
                input_files.push_back(source.filepath);
            }
            // starting new file
            else if (source.filepath != input_files.back()) {
                // should push deep copy, so clear is safe
                row_groups.push_back(rgs);
                rgs.clear();
                input_files.push_back(source.filepath);
            }
            // otherwise just continuing current file
            rgs.push_back(source.rgidx);
        }
    }

    // no matches?
    if (input_files.size() != 0) {
        // push final batch
        row_groups.push_back(rgs);
        size_t startFreeMem = getFreeMem();
        std::cerr << "Load..." << std::flush;
        // process each file separately
        for (::size_t idx = 0; idx < input_files.size(); ++idx) {
            std::unique_ptr<cudf::io::datasource> fileSource = cudf::io::datasource::create(input_files[idx]);
            std::unique_ptr<S3WaitingSource> waitingSource{};
            std::unique_ptr<cudf::io::source_info> si{};
            if (usesS3Input) {
                waitingSource = std::make_unique<S3WaitingSource>(std::move(fileSource), downloadedAmount);
                si = std::make_unique<cudf::io::source_info>(waitingSource.get());
            } else {
                si = std::make_unique<cudf::io::source_info>(input_files[idx]);
            }
            auto [status, result] = load_sort(*si, {row_groups[idx]}, low, high, sort_col);

            if (status == MergeResult::Success) {
                if (result) {
                    sortedTables.push_back(std::move(result));
                }
            } else {
                throw std::runtime_error("Partition too big");
            }
        }
        memUsed = (startFreeMem - getFreeMem());
    }
    if (!sortedTables.empty()) {
        // create vector of table views
        std::vector<cudf::table_view> tviews;
        for (size_t t = 0; t < sortedTables.size(); t++) {
            tviews.push_back(sortedTables[t]->view());
        }
        std::cerr << "Merge..." << std::flush;
        auto merged = cudf::merge(tviews, {sort_col}, {cudf::order::ASCENDING});
        return {std::move(merged), memUsed};
    }
    // nothing to merge
    return {nullptr, memUsed};
}

class Test : public arrow::ScalarVisitor {
    template <typename T>
    arrow::Status Visit(T) {
        return arrow::Status::OK();
    }
};

template <typename T>
row_mem_t write_range_low_mem(std::vector<fileinfo_t> const& sources, cudf::size_type const sort_col,
                              T const& low, T const& high,
                              std::vector<EdgePair_vec<T>> const& filestats,
                              std::shared_ptr<std::atomic_size_t> downloadedAmount,
                              std::unique_ptr<cudf::io::parquet_chunked_writer>& writer) {
    tab_mem_t tableMem = table_for_range_low_mem(sources, sort_col, low, high,
                                                 filestats, downloadedAmount);
    if (tableMem.first.get() == nullptr) {
        return {0, 0};
    }

    std::cerr << "Write" << std::endl;
    writer->write(tableMem.first->view());

    cudf::column_view const& firstColumn = tableMem.first->get_column(0).view();
    std::vector<cudf::column_view> sliced = cudf::slice(firstColumn, {0, 1, firstColumn.size() - 1, firstColumn.size()});
    std::vector<std::unique_ptr<cudf::column>> column;
    column.push_back(cudf::concatenate({sliced}));
    cudf::table tabled{std::move(column)};
    // pull this back to cpu
    std::shared_ptr<arrow::Table> cpuTable = cudf::to_arrow(tabled.view(), {{"a"}});
    std::shared_ptr<arrow::ChunkedArray> cpuColumn = cpuTable->column(0);
    arrow::Result<std::shared_ptr<arrow::Scalar>> firstVal = cpuColumn->GetScalar(0);
    if (firstVal.ok()) {
        std::shared_ptr<arrow::Scalar> val = firstVal.ValueOrDie();
        Test visit{};
        val->Accept(&visit);
    }

    freeMemory("write tables done");
    return {static_cast<size_t>(tableMem.first->num_rows()), tableMem.second};
}

template <typename T>
::size_t checkForOutputSplit(std::vector<EdgePair<T>> const& ranges, ::size_t const endIndex, std::vector<T> const& outputSplits) {
    // check to see if there is a output split point somewhere in this range
    if (!outputSplits.empty()) {
        for (::size_t idx = 0; idx < endIndex; ++idx) {
            EdgePair<T> const& edge = ranges[idx];
            if (std::binary_search(outputSplits.cbegin(), outputSplits.cend(), edge.second)) {
                return idx;
            }
        }
    }
    return endIndex;
}

template <typename T, typename ParquetStatsType = typename parquet_type_map::convert<T>>
void doReadRowGroupStats(std::vector<cudf::size_type> const& sortCols, std::shared_ptr<parquet::FileMetaData> const fmeta, std::vector<EdgePair_vec<T>>& fileStats, std::vector<fileinfo_t>& sources,
                         Edge_vec<T>& rowGroupParts, std::string const& file, ::size_t const fileIndex) {
    Edge_vec<T> fileGroupParts{};
    EdgePair_vec<T> rgstats;
    for (int c = 0; c < fmeta->num_row_groups(); c++) {
        auto rgmeta = fmeta->RowGroup(c);
        auto stats = std::static_pointer_cast<ParquetStatsType>(rgmeta->ColumnChunk(sortCols[0])->statistics());
        T min, max;
        if constexpr (std::is_integral_v<T>) {
            min = stats->min();
            max = stats->max();
        } else {  // string type
            min = {reinterpret_cast<const char*>(stats->min().ptr), stats->min().len};
            max = {reinterpret_cast<const char*>(stats->max().ptr), stats->max().len};
        }
        rgstats.emplace_back(min, max);
        // add the current row group to the list of group partitions
        if (c == 0) {  // special case for first entry.
            fileGroupParts.push_back({min, 0});
        }
        fileGroupParts.push_back({max, static_cast<::size_t>(rgmeta->total_byte_size())});
        sources.push_back({file, fileIndex, c});
    }
    std::cerr << file << " bytes " << totalEdges(fileGroupParts.cbegin(), fileGroupParts.cend())
              << " row groups " << fmeta->num_row_groups() << " rows " << fmeta->num_rows() << std::endl;

    rowGroupParts = mergeCombine(rowGroupParts, fileGroupParts);
    fileStats.push_back(std::move(rgstats));
}

void readRowGroupStats(std::vector<cudf::size_type> const& sortCols, std::shared_ptr<parquet::FileMetaData> const fmeta,
                       SupportedTypes::EdgePair_vec2_variant& fileStatsVariant,
                       std::vector<fileinfo_t>& sources, SupportedTypes::Edge_vec_variant& rowGroupParts,
                       std::string const& file, ::size_t const fileIndex) {
    std::visit([&](auto&& arg) {
        using ArgType = std::decay_t<decltype(arg)>;          // determine underlying type of variant -> Edge_vec<T>
        using BaseType = typename ArgType::value_type::type;  // get the underlying type of T in the above line
        doReadRowGroupStats(sortCols, fmeta, std::get<std::vector<EdgePair_vec<BaseType>>>(fileStatsVariant), sources, arg, file, fileIndex);
    },
               rowGroupParts);
}

template <typename T>
CommandLineOutput doCompactFiles(Edge_vec<T> const& rowGroupParts, CommandLineInput const& opts, std::vector<fileinfo_t> const& sources, std::vector<EdgePair_vec<T>> const& fileStats,
                                 std::shared_ptr<AwsLibrary> awsPtr, cudf::io::table_input_metadata const& tim, std::shared_ptr<S3Sink>& currentSink, std::shared_ptr<std::atomic_size_t> downloadedAmount) {
    CommandLineOutput returnData{};
    // want to work from total available memory, not total device memory
    ::size_t totalMem = getFreeMem();
    // Create list of partitions based on the merged histograms of all input files.
    // Make each partition small based on row group size. We will then start merging
    // multiple partitions at once, based on available memory
    ::size_t partSize = (totalMem / 2) * 0.001;
    auto ranges = makePartitions(rowGroupParts, partSize);
    std::vector<T> outputSplits = convertVector<T>(opts.splitPoints);
    insertToEdgeList(ranges, outputSplits);
    T const globMin = ranges.front().first;
    T const globMax = ranges.back().second;

    std::cerr << "Partition size " << partSize << std::endl;
    std::cerr << "Global minimum " << globMin << " maximum " << globMax << std::endl;
    // increment last character of last range to make sure it is exclusive when we perform the merge (or increment if integral)
    if constexpr (std::is_integral_v<T>) {
        ranges.back().second++;
    } else {
        ranges.back().second.back()++;
    }

    std::unique_ptr<cudf::io::parquet_chunked_writer> writer = createWriter(opts, tim, (awsPtr) ? awsPtr.get() : nullptr, currentSink);

    // Memory limit control
    // ====================
    // the maximum number of ranges per partition to hopefully prevent OOM
    ::size_t maxSafeRangesPerPart = std::numeric_limits<::size_t>::max();
    // previous ranges per part count. We only set the maxSafeRanges limit if the max safe
    // ranges is on an increasing trajectory (positive gradient)
    ::size_t previousRangesPerPart = 1;
    // ====================
    // row count written
    ::size_t count = 0;
    // number of partitions to merge in one pass
    ::size_t rangesPerPart = 1;
    while (!ranges.empty()) {
        EdgePair<T>& curr = ranges.front();
        // find ending point based on how many ranges in this partition
        ::size_t endIndex = std::min(rangesPerPart, ranges.size()) - 1;
        // now check to see if this range includes an output split point
        endIndex = checkForOutputSplit(ranges, endIndex, outputSplits);
        // update ranges in case the index changed
        rangesPerPart = endIndex + 1;
        std::cerr << "RangesPerPart " << rangesPerPart << " index 0-" << endIndex << "\n";
        EdgePair<T>& endRange = ranges[endIndex];
        bool newOutputNeeded = (!outputSplits.empty() && std::binary_search(outputSplits.cbegin(), outputSplits.cend(), endRange.second));
        std::cerr << "Attempt range [ " << curr.first << ", " << endRange.second << " )\n";

        ::size_t nextRangesPerPart = 0;
        try {
            auto [partCount, memUsed] = write_range_low_mem(sources, opts.sortIndexes[0], curr.first, endRange.second, fileStats, downloadedAmount, writer);
            count += partCount;
            // remove the parts we've just done
            ranges.erase(ranges.begin(), ranges.begin() + endIndex + 1);

            // how many parts next time?
            ::size_t avgPartSize = (memUsed / (endIndex + 1)) * 2;
            if (memUsed > 0) {
                nextRangesPerPart = (totalMem / 2) / avgPartSize;
                nextRangesPerPart = (rangesPerPart + nextRangesPerPart) / 2;
                std::cerr << "Used memory " << memUsed << " average part size " << avgPartSize
                          << " suggested next range attempt " << nextRangesPerPart;
                // check we won't break limit, don't trigger if new limit set this loop
                if (nextRangesPerPart > maxSafeRangesPerPart) {
                    nextRangesPerPart = maxSafeRangesPerPart;
                    std::cerr << " limit hit, reducing to " << nextRangesPerPart;
                }
            } else {
                // nothing written...
                std::cerr << "no rows match, doubling range of partitions to merge ";
                nextRangesPerPart = rangesPerPart << 1;
            }

            // do we need to start a new file?
            if (newOutputNeeded) {
                writer->close();
                writer = createWriter(opts, tim, (awsPtr) ? awsPtr.get() : nullptr, currentSink);
                returnData.minKeys.push_back("a");  // TODO set these correctly
                returnData.maxKeys.push_back("z");
                returnData.rowsWritten.push_back(static_cast<std::uint64_t>(count));
                count = 0;
            }
        } catch (std::exception& excpt) {
            std::cerr << excpt.what() << std::endl;
            // are we on positive gradient?
            if (rangesPerPart > previousRangesPerPart) {
                maxSafeRangesPerPart = rangesPerPart * 0.90;
                std::cerr << " set safe limit on range size to " << maxSafeRangesPerPart;
            }
            nextRangesPerPart = rangesPerPart >> 1;
            std::cerr << " Processing partition failed, reducing range of partitions to merge...";
        }
        previousRangesPerPart = rangesPerPart;
        rangesPerPart = nextRangesPerPart;
        std::cerr << " next attempt " << rangesPerPart << std::endl;
    }
    writer->close();
    returnData.minKeys.push_back("a");  // TODO set these correctly
    returnData.maxKeys.push_back("z");
    returnData.rowsWritten.push_back(static_cast<std::uint64_t>(count));
    if constexpr (std::is_integral_v<T>) {
        // return {std::to_string(globMin), std::to_string(globMax), 0, count};
    } else {
        // return {globMin, globMax, 0, count};
    }
    return returnData;
}

CommandLineOutput compactFiles(SupportedTypes::Edge_vec_variant const& rowGroupParts, CommandLineInput const& opts,
                               std::vector<fileinfo_t> const& sources, SupportedTypes::EdgePair_vec2_variant const& fileStats,
                               std::shared_ptr<AwsLibrary> awsPtr, cudf::io::table_input_metadata const& tim,
                               std::shared_ptr<S3Sink>& currentSink, std::shared_ptr<std::atomic_size_t> downloadedAmount) {
    return std::visit([&](auto&& arg) -> CommandLineOutput {
        using ArgType = std::decay_t<decltype(arg)>;          // determine underlying type of variant -> Edge_vec<T>
        using BaseType = typename ArgType::value_type::type;  // get the underlying type of T in the above line
        return doCompactFiles(arg, opts, sources, std::get<std::vector<EdgePair_vec<BaseType>>>(fileStats), awsPtr, tim, currentSink, downloadedAmount);
    },
                      rowGroupParts);
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "usage: " << argv[0] << " [output file] [parts_file] [input file ...]\n"
                                             "If S3 URIs are given, then the input file can be a prefix where all objects in that prefix will be\n"
                                             "downloaded.\n"
                                             "If the output file is \"-\" then read and write MessagePack from second argument file.\n";
        return 1;
    }

    // prevent SIGPIPE from terminating program
    std::signal(SIGPIPE, SIG_IGN);

    // have comma separated output of values
    std::locale comma_locale(std::locale(), new comma_numpunct());
    std::cerr.imbue(comma_locale);

    // force gpu initialization so it's not included in the time
    rmm::cuda_stream_default.synchronize();

    // The S3 connection if we need it. Make this a shared_ptr?
    std::shared_ptr<AwsLibrary> awsPtr = std::make_shared<AwsLibrary>();
    // yuck!
    std::shared_ptr<S3Sink> currentSink;
    // This is a bit ugly....this could do with being encapsulated in a class somewhere...
    std::shared_ptr<std::atomic_size_t> downloadedAmount{};

    CommandLineInput opts{};
    if (argv[1] == std::string("-")) {
        // the iterator approach is safe since we are reading chars and writing to a uint8_t data buffer
        // and implicit well defined conversions exist.
        std::ifstream partFile{argv[2]};
        std::vector<std::uint8_t> inputMsg{std::istream_iterator<char>{partFile}, std::istream_iterator<char>{}};
        opts = msgpack::unpack<CommandLineInput>(inputMsg);
    } else {
        opts.outputFiles = {argv[1]};
        // load the output file split points from partition file (if it's valid)
        opts.splitPoints = loadOutputPartsFile(argv[2]);
        opts.inputFiles = {argv + 3, argv + argc};
    }
    for (auto const & s:opts.splitPoints) {
        std::cerr << s << std::endl;
    }

    if (opts.outputFiles.size() != opts.splitPoints.size() + 1) {
        std::cerr << opts.splitPoints.size() << " splits need " << opts.splitPoints.size() + 1 << " output files specified, only " << opts.outputFiles.size() << " were given.\n";
        return 1;
    }
    if (opts.inputFiles.empty()) {
        std::cerr << "No input files!\n";
        return 1;
    }
    if (isS3Path(opts.inputFiles[0])) {
        usesS3Input = true;
        // create list of local files that are being downloaded from S3 and start
        // them downloading in the background
        std::tie(opts.inputFiles, downloadedAmount) = initialiseFromS3(*awsPtr, PARQUET_EXTENSION, BATCH_SIZE, DEFAULT_FOOTER_SIZE, opts.inputFiles);
    } else {
        // start background thread to prefetch all the files from memory
        auto thread = std::thread{start_prefetchers, std::cref(opts.inputFiles)};
        thread.detach();
    }

    std::vector<fileinfo_t> sources;
    ::size_t totalRows = 0;
    // get row group stats on a per-file basis
    SupportedTypes::EdgePair_vec2_variant filestats;
    // partitions on primary sort field for partitioning
    SupportedTypes::Edge_vec_variant rowGroupParts;
    // need to create table_input_metadata to get column names and nullability correct
    cudf::io::table_input_metadata tim;

    for (::size_t i = 0; i < opts.inputFiles.size(); ++i) {
        std::string const& file = opts.inputFiles[i];
        auto filef = ::arrow::io::ReadableFile::Open(file);
        std::shared_ptr<parquet::FileMetaData> fmeta = parquet::ReadMetaData(*filef);
        totalRows += fmeta->num_rows();
        if (i == 0) {  // get schema from first file/*  */
            ::size_t variantIndex = findVariantIndex(fmeta, opts);
            filestats = populateVariant<SupportedTypes::EdgePair_vec2_variant>(variantIndex);
            rowGroupParts = populateVariant<SupportedTypes::Edge_vec_variant>(variantIndex);
            const parquet::SchemaDescriptor* schema = fmeta->schema();
            for (int c = 0; c < schema->num_columns(); c++) {
                bool nullable = schema->Column(c)->max_definition_level() > 0;
                std::string name = schema->Column(c)->name();
                cudf::io::column_in_metadata cim(name);
                cim.set_nullability(nullable);
                tim.column_metadata.push_back(cim);
            }
        }
        readRowGroupStats(opts.sortIndexes, fmeta, filestats, sources, rowGroupParts, file, i);
    }
    std::visit([&](auto&& arg) {
        std::cerr << "Total bytes in " << opts.inputFiles.size() << " files " << totalEdges(arg.cbegin(), arg.cend())
                  << " rows " << totalRows << std::endl;
    },
               rowGroupParts);

    double tstart = timestamp();
    CommandLineOutput resultData = compactFiles(rowGroupParts, opts, sources, filestats, awsPtr, tim, currentSink, downloadedAmount);
    double tend = timestamp();

    std::cerr << "total num rows written " << std::accumulate(resultData.rowsWritten.cbegin(), resultData.rowsWritten.cend(), 0) << std::endl;
    std::cerr << "total time " << (tend - tstart) << " seconds" << std::endl;
    // write out message pack data
    resultData.rowsRead = totalRows;
    std::vector<std::uint8_t> msgOut = msgpack::pack(resultData);
    std::copy(msgOut.cbegin(), msgOut.cend(), std::ostream_iterator<char>{std::cout});
    return 0;
}
