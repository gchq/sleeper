#!/usr/bin/env python3

import argparse
import dataclasses
import io
import sys

import msgpack

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
