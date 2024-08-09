#pragma once

#include <cstddef>
#include <parquet_types.h>

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