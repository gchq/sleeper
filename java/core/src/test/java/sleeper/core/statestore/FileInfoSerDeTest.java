/*
 * Copyright 2022-2024 Crown Copyright
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

package sleeper.core.statestore;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.type.StringType;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class FileInfoSerDeTest {

    @Test
    public void shouldSerDeRootFile() {
        // Given
        FileInfoFactory fileInfoFactory = FileInfoFactory.from(
                new PartitionsBuilder(schemaWithKey("key")).singlePartition("root").buildTree());
        FileInfo file = fileInfoFactory.rootFile("test.parquet", 100);
        FileInfoSerDe serde = new FileInfoSerDe();

        // When
        FileInfo read = serde.fromJson(serde.toJson(file));

        // Then
        assertThat(read).isEqualTo(file);
    }

    @Test
    public void shouldSerDeSplitFile() {
        // Given
        FileInfoFactory fileInfoFactory = FileInfoFactory.from(
                new PartitionsBuilder(schemaWithKey("key", new StringType()))
                        .rootFirst("root")
                        .splitToNewChildren("root", "L", "R", "aaa")
                        .buildTree());
        FileInfo rootFile = fileInfoFactory.rootFile("test.parquet", 100);
        FileInfo leftFile = SplitFileInfo.referenceForChildPartition(rootFile, "L");
        FileInfo rightFile = SplitFileInfo.referenceForChildPartition(rootFile, "R");
        FileInfoSerDe serde = new FileInfoSerDe();

        // When
        List<FileInfo> read = serde.listFromJson(serde.listToJson(List.of(leftFile, rightFile)));

        // Then
        assertThat(read).containsExactly(leftFile, rightFile);
    }
}
