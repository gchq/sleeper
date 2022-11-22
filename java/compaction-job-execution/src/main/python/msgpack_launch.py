#!/usr/bin/env python3

import argparse
import dataclasses
import io
import sys

import msgpack

MAX_ROW_GROUP_BYTES = 128*1024*1024
MAX_ROW_GROUP_ROWS = 1_000_000
PAGE_SIZE_BYTES = 512*1024
CODEC = "SNAPPY"


@dataclasses.dataclass
class CommandLineInput:
    inputFiles: list[str]
    outputFiles: list[str]
    codec: str
    rowGroupBytes: int
    rowGroupRows: int
    pageBytes: int
    sortIndexes: list[int]
    splitPoints: list[str]

    def serialise(self, stream: io.BytesIO):
        msgpack.pack(self.inputFiles, stream)
        msgpack.pack(self.outputFiles, stream)
        msgpack.pack(self.codec, stream)
        msgpack.pack(self.rowGroupBytes, stream)
        msgpack.pack(self.rowGroupRows, stream)
        msgpack.pack(self.pageBytes, stream)
        msgpack.pack(self.sortIndexes, stream)
        msgpack.pack(self.splitPoints, stream)


def _process_args(argv: list[str] = None):
    """
    Processes command line args. Uses sys.argv is none are passed in

    :param argv: argument list
    :return: processed args
    """
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser(
        description="Writes a MessagePack output suitable for consuming with GPU compactor. File names may be local or start with \"s3://\"")
    parser.add_argument("input_file", type=str, action='store', nargs='+',
                        help="path to Parquet file(s) to compact")
    parser.add_argument("--output", "-o", action="extend", type=str,
                        required=True, nargs='+', help="path to output parquet file(s)")
    parser.add_argument("--codec", "-c", action="store",
                        type=str, help="Output compression codec", default=CODEC)
    parser.add_argument("--row-group-bytes", "-r", action="store", type=int,
                        help="Maximum row group size in bytes", default=MAX_ROW_GROUP_BYTES)
    parser.add_argument("--row-group-rows", "-t", action="store", type=int,
                        help="Maximum row group size in rows", default=MAX_ROW_GROUP_ROWS)
    parser.add_argument("--page-bytes", "-p", action="store", type=int,
                        help="Maximum page size in bytes", default=PAGE_SIZE_BYTES)
    parser.add_argument("--sort-column", "-s", nargs='+', action="extend",
                        type=int, help="Use multiple times. Column number to sort on")
    parser.add_argument("-d", type=str, action="extend", nargs='+',
                        help="Use multiple times. Specify a split point for output")
    args = parser.parse_args(argv)
    if args.sort_column is None:
        args.sort_column = [0]
    return args


def main():
    args = _process_args()
    # Flatten the lists of command line arguments
    settings = CommandLineInput(args.input_file, args.output, args.codec, args.row_group_bytes,
                                args.row_group_rows, args.page_bytes, args.sort_column, args.d)
    buf = io.BytesIO()
    settings.serialise(buf)
    sys.stdout.buffer.write(buf.getvalue())


if __name__ == "__main__":
    main()
