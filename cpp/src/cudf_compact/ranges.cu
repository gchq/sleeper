
#include "cudf_compact/ranges.hpp"

#include <rmm/cuda_stream_view.hpp>
#include <rmm/device_uvector.hpp>
#include <thrust/binary_search.h>
#include <thrust/iterator/constant_iterator.h>
#include <thrust/iterator/discard_iterator.h>
#include <thrust/sequence.h>
#include <thrust/sort.h>

#include "cudf_compact/filters.hpp"

#include <iostream>
#include <vector>

namespace gpu_compact::cudf_compact
{

template<typename UnaryFunction>
inline __device__ auto make_counting_transform_iterator(cudf::size_type start, UnaryFunction f) {
    return thrust::make_transform_iterator(thrust::make_counting_iterator(start), f);
}

struct row_total_size
{
    page_info const *cum_pages;
    int const *key_offsets;
    size_t num_keys;

    __device__ inline page_info operator()(page_info const &i) {
        // sum sizes for each input column at this row
        size_t sum = 0;
        // iterate over each global_col_idx
        for (int idx = 0; idx < num_keys; idx++) {
            auto const start = key_offsets[idx];
            auto const end = key_offsets[idx + 1];
            auto iter = make_counting_transform_iterator(0, [&] __device__(int i) { return cum_pages[i].row_count; });
            // for the number of rows in i, find where in the list of pages that number of rows should be inserted
            auto const page_index = thrust::lower_bound(thrust::seq, iter + start, iter + end, i.row_count) - iter;
            // add the size of all those pages to that point for this column to the total size
            sum += cum_pages[page_index].size_bytes;
        }
        // so now we know for the given page i, what the total size of all rows across all files is to that point
        return { i.file_idx, i.rg_idx, i.col_idx, i.page_idx, i.schema_idx, i.global_col_idx, i.row_count, sum };
    }
};

struct page_info_by_index
{
    page_info *data;

    __device__ inline page_info operator()(int index) {
        return data[index];
    }
};

std::deque<scalar_pair> getRanges(std::vector<page_info> const &pages,
  cudf::size_type sort_col,
  parquet::format::Type::type col_type,
  parquet::format::ConvertedType::type conv_type,
  size_t chunk_read_limit,
  std::vector<std::vector<parquet::format::ColumnIndex>> const &indexes_per_file) {
    auto stream = rmm::cuda_stream_default;

    // create page keys (transformed into global column index) and copy to device
    std::vector<int> h_page_keys(pages.size());
    std::transform(
      pages.begin(), pages.end(), h_page_keys.begin(), [](auto const &page) { return page.global_col_idx; });

    // Copy to device
    rmm::device_uvector<int> page_keys(pages.size(), stream);
    cudaMemcpyAsync(page_keys.data(), h_page_keys.data(), sizeof(int) * h_page_keys.size(), cudaMemcpyDefault, stream);

    // Create device numerical sequence from 0
    rmm::device_uvector<int> page_index(page_keys.size(), stream);
    thrust::sequence(thrust::device, page_index.begin(), page_index.end());

    // Sort key/value with global column index as key and page idx as value. Therefore all pages indexes for a single
    // column are contiguous in page_index. page_keys will now contain contiguous runs of global_col_idxs, e.g. all 0's,
    // followed by all 1's, etc.
    thrust::stable_sort_by_key(
      thrust::device, page_keys.begin(), page_keys.end(), page_index.begin(), thrust::less<int>());

    // copy page_info vector to device
    rmm::device_uvector<page_info> d_pages(pages.size(), stream);
    cudaMemcpyAsync(d_pages.data(), pages.data(), sizeof(page_info) * pages.size(), cudaMemcpyDefault, stream);

    rmm::device_uvector<page_info> cum_pages(page_keys.size(), stream);
    // Make an iterator of the page indexes that will return the page_info object
    auto page_input = thrust::make_transform_iterator(page_index.begin(), page_info_by_index{ d_pages.data() });

    // Fill cum_pages vector with page_info's where each successive object contains the cumulative row count and data
    // size and order will be file 0, col 0, col 1, ..., col N, file 1 col 0, col 1, ..., col N, ...
    thrust::inclusive_scan_by_key(thrust::device,
      page_keys.begin(),
      page_keys.end(),
      page_input,
      cum_pages.begin(),
      thrust::equal_to{},
      [] __device__(auto const &a, auto const &b) {
          return page_info{ b.file_idx,
              b.rg_idx,
              b.col_idx,
              b.page_idx,
              b.schema_idx,
              b.global_col_idx,
              a.row_count + b.row_count,
              a.size_bytes + b.size_bytes };
      });

    // Now sort that to a new vector by row count
    rmm::device_uvector<page_info> cum_pages_sorted{ cum_pages, stream };
    thrust::sort(thrust::device,
      cum_pages_sorted.begin(),
      cum_pages_sorted.end(),
      [] __device__(page_info const &a, page_info const &b) { return a.row_count < b.row_count; });

    rmm::device_uvector<int> key_offsets(page_keys.size() + 1, stream);
    // Work out how many pages per global_col_idx, e.g. how many pages in file 0 col 0, file 0 col 1, ..., col N,
    // file 1 col 0, file 1 col 1, ...

    // key_offsets_end is iterator positioned at end of filled part of vector
    auto const key_offsets_end = thrust::reduce_by_key(thrust::device,
      page_keys.begin(),
      page_keys.end(),
      thrust::make_constant_iterator(1),
      thrust::make_discard_iterator(),
      key_offsets.begin())
                                   .second;

    // Number of cols * number of files
    size_t const num_unique_keys = key_offsets_end - key_offsets.begin();

    // Reductive sum (first element 0) of key_offsets to get final result, key_offsets gives you index
    // into cum_pages where each new column starts
    thrust::exclusive_scan(thrust::device, key_offsets.begin(), key_offsets.end(), key_offsets.begin());

    // Working from cum_pages_sorted which is sorted based on cumulative row count, create vector of pages with size
    // set to the total size to that row position
    rmm::device_uvector<page_info> aggregated_info(cum_pages.size(), stream);
    thrust::transform(thrust::device,
      cum_pages_sorted.begin(),
      cum_pages_sorted.end(),
      aggregated_info.begin(),
      row_total_size{ cum_pages.data(), key_offsets.data(), num_unique_keys });

    // Just keep the pages for the sorting column
    rmm::device_uvector<page_info> d_filtered_pages(aggregated_info.size(), stream);
    auto filtered_end = thrust::copy_if(thrust::device,
      aggregated_info.begin(),
      aggregated_info.end(),
      d_filtered_pages.begin(),
      [sort_col] __device__(auto const &pg) { return pg.schema_idx == sort_col; });
    d_filtered_pages.resize(std::distance(d_filtered_pages.begin(), filtered_end), stream);

    // bring filtered_pages to host for last step
    std::vector<page_info> filtered_pages(d_filtered_pages.size());
    cudaMemcpyAsync(filtered_pages.data(),
      d_filtered_pages.data(),
      sizeof(page_info) * d_filtered_pages.size(),
      cudaMemcpyDefault,
      stream);

    // wait for all pending operations
    stream.synchronize();

    std::deque<scalar_pair> ranges;
    size_t cur_pos = 0;
    size_t cur_cumulative_size = 0;
    size_t cur_row_count = 0;
    auto start = thrust::make_transform_iterator(
      filtered_pages.begin(), [&](page_info const &i) { return i.size_bytes - cur_cumulative_size; });
    auto end = start + filtered_pages.size();

    auto last_scalar = min_for_type(col_type, conv_type);
    auto max_scalar = max_for_type(col_type, conv_type);

    std::string last_val = "-inf";
    std::string const max_val = "inf";
    while (true) {
        int64_t split_pos = thrust::lower_bound(thrust::seq, start + cur_pos, end, chunk_read_limit) - start;

        // if we're past the end, or if the returned bucket is > than the chunk_read_limit, move
        // back one.
        if (static_cast<size_t>(split_pos) >= filtered_pages.size()
            || (filtered_pages[split_pos].size_bytes - cur_cumulative_size > chunk_read_limit)) {
            split_pos--;
        }

        // best-try. if we can't find something that'll fit, we have to go bigger. we're doing
        // this in a loop because all of the cumulative sizes for all the pages are sorted into
        // one big list. so if we had two columns, both of which had an entry {1000, 10000},
        // that entry would be in the list twice. so we have to iterate until we skip past all
        // of them.  The idea is that we either do this, or we have to call unique() on the
        // input first.
        while (split_pos < (static_cast<int64_t>(filtered_pages.size()) - 1)
               && (split_pos < 0 || filtered_pages[split_pos].row_count == cur_row_count)) {
            split_pos++;
        }
        auto const start_row = cur_row_count;
        cur_row_count = filtered_pages[split_pos].row_count;

        if (cur_row_count == start_row)
            break;

        cur_pos = split_pos;
        cur_cumulative_size = filtered_pages[split_pos].size_bytes;

        auto const &splt = filtered_pages[split_pos];
        auto const &colidx = indexes_per_file[splt.file_idx][splt.col_idx];
        auto const &min = colidx.min_values[splt.page_idx];
        auto const &end_val = split_pos == filtered_pages.size() - 1 ? max_val : min;
        auto end_sclr = to_scalar(min, col_type, conv_type);

        ranges.emplace_back(scalar_pair{ to_string(last_val, col_type, conv_type),
          last_scalar,
          to_string(end_val, col_type, conv_type),
          split_pos == filtered_pages.size() - 1 ? max_scalar : end_sclr });

        std::string rangeBegin = to_string(last_val, col_type, conv_type);
        std::string rangeEnd = to_string(end_val, col_type, conv_type);

        std::cout << "Adding range \"" << rangeBegin << "\"->\"" << rangeEnd << "\"\n";

        last_val = min;
        last_scalar = end_sclr;
    }

    return ranges;
}

}// namespace gpu_compact::cudf_compact