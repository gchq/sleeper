/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.clients.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;

import sleeper.clients.util.console.ConsoleOutput;

import java.io.IOException;
import java.io.PrintStream;

/**
 * A utility class to show the page indexes in a Parquet file.
 */
public class ShowPageIndexes {

    private final ConsoleOutput out;

    private ShowPageIndexes() {
        this(System.out);
    }

    ShowPageIndexes(PrintStream out) {
        this.out = new ConsoleOutput(out);
    }

    public void run(String file) throws IOException {
        int rowGroup = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(file), new Configuration()))) {
            ParquetMetadata footer = reader.getFooter();
            for (BlockMetaData blockMetaData : footer.getBlocks()) {
                out.println("Row group " + rowGroup);
                for (ColumnChunkMetaData columnChunkMetaData : blockMetaData.getColumns()) {
                    String path = columnChunkMetaData.getPath().toDotString();
                    out.println("Column index for column " + path);
                    ColumnIndex columnIndex = reader.readColumnIndex(columnChunkMetaData);
                    out.println(columnIndex.toString());
                    out.println("Page index for column " + path);
                    OffsetIndex pageIndex = reader.readOffsetIndex(columnChunkMetaData);
                    out.println(pageIndex.toString());
                }
                rowGroup++;
            }
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Wrong number of arguments");
            System.out.println("Usage: <path-to-file>");
            System.exit(1);
        }
        new ShowPageIndexes().run(args[0]);
    }
}
