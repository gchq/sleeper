#pragma once

#include "cudf_compact/parquet_types.h"

#include <cstddef>
#include <iosfwd>
struct col_schema
{
    parquet::format::SchemaElement se;
    int max_def;
    int max_rep;
};

struct page_info
{
    int file_idx;// file this page comes from
    int rg_idx;// row group this page comes from
    int col_idx;// column index within file this page comes from
    int page_idx;// index of page within the column
    int schema_idx;// schema index for this column
    int global_col_idx;// column index wrt all files (key)
    int row_count;
    size_t size_bytes;
};

inline std::ostream &operator<<(std::ostream &out, page_info const &p)
{
    return out << "file_idx " << p.file_idx << " rg_idx " << p.rg_idx << " col_idx " << p.col_idx << " page_idx "
               << p.page_idx << " schema_idx " << p.schema_idx << " global_col_idx " << p.global_col_idx
               << " row_count " << p.row_count << " size_bytes " << p.size_bytes;
}