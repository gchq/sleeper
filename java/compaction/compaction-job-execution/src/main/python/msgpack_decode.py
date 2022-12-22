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

def _process_args(argv: list[str] = None):
    """
    Processes command line args. Uses sys.argv is none are passed in

    :param argv: argument list
    :return: processed args
    """
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser(
        description="Tests the MessagePack encoded output produced by msgpack_launch.py")
    parser.add_argument("file", action="store",help="MsgPack encoded file",type=argparse.FileType("rb"))
    args = parser.parse_args(argv)
    return args

def main():
    args = _process_args()
    unpacker = msgpack.Unpacker(args.file)
    for object in unpacker:
        print(object)
    


if __name__ == "__main__":
    main()
