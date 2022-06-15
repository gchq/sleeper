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
package sleeper.compaction.job;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class CompactionJobTest {

    @Test
    public void testEqualsAndHashCodeForNonSplittingJobWithNoIterator() {
       // Given
       CompactionJob compactionJob1 = new CompactionJob("table", "job-1");
       compactionJob1.setInputFiles(Arrays.asList("file1", "file2", "file3"));
       compactionJob1.setOutputFile("outputFile");
       compactionJob1.setIsSplittingJob(false);
       compactionJob1.setPartitionId("partition1");
       CompactionJob compactionJob2 = new CompactionJob("table", "job-1");
       compactionJob2.setInputFiles(Arrays.asList("file1", "file2", "file3"));
       compactionJob2.setOutputFile("outputFile");
       compactionJob2.setIsSplittingJob(false);
       compactionJob2.setPartitionId("partition1");
       CompactionJob compactionJob3 = new CompactionJob("table", "job-2");
       compactionJob3.setInputFiles(Arrays.asList("file1", "file2", "file3"));
       compactionJob3.setOutputFile("outputFile2");
       compactionJob3.setIsSplittingJob(false);
       compactionJob3.setPartitionId("partition1");
       CompactionJob compactionJob4 = new CompactionJob("table2", "job-2");
       compactionJob4.setInputFiles(Arrays.asList("file1", "file2", "file3"));
       compactionJob4.setOutputFile("outputFile2");
       compactionJob4.setIsSplittingJob(false);
       compactionJob4.setPartitionId("partition1");
       
       // When
       boolean equals1 = compactionJob1.equals(compactionJob2);
       boolean equals2 = compactionJob1.equals(compactionJob3);
       boolean equals3 = compactionJob3.equals(compactionJob4);
       int hashCode1 = compactionJob1.hashCode();
       int hashCode2 = compactionJob2.hashCode();
       int hashCode3 = compactionJob3.hashCode();
       int hashCode4 = compactionJob4.hashCode();
       
       // Then
       assertTrue(equals1);
       assertFalse(equals2);
       assertFalse(equals3);
       assertEquals(hashCode1, hashCode2);
       assertNotEquals(hashCode1, hashCode3);
       assertNotEquals(hashCode3, hashCode4);
    }
    
    @Test
    public void testEqualsAndHashCodeForNonSplittingJobWithIterator() {
       // Given
       CompactionJob compactionJob1 = new CompactionJob("table", "job-1");
       compactionJob1.setInputFiles(Arrays.asList("file1", "file2", "file3"));
       compactionJob1.setOutputFile("outputFile");
       compactionJob1.setIsSplittingJob(false);
       compactionJob1.setPartitionId("partition1");
       compactionJob1.setIteratorClassName("Iterator.class");
       compactionJob1.setIteratorConfig("config1");
       CompactionJob compactionJob2 = new CompactionJob("table", "job-1");
       compactionJob2.setInputFiles(Arrays.asList("file1", "file2", "file3"));
       compactionJob2.setOutputFile("outputFile");
       compactionJob2.setIsSplittingJob(false);
       compactionJob2.setPartitionId("partition1");
       compactionJob2.setIteratorClassName("Iterator.class");
       compactionJob2.setIteratorConfig("config1");
       CompactionJob compactionJob3 = new CompactionJob("table", "job-1");
       compactionJob3.setInputFiles(Arrays.asList("file1", "file2", "file3"));
       compactionJob3.setOutputFile("outputFile");
       compactionJob3.setIsSplittingJob(false);
       compactionJob3.setPartitionId("partition1");
       compactionJob3.setIteratorClassName("Iterator2.class");
       compactionJob3.setIteratorConfig("config1");
       
       // When
       boolean equals1 = compactionJob1.equals(compactionJob2);
       boolean equals2 = compactionJob1.equals(compactionJob3);
       int hashCode1 = compactionJob1.hashCode();
       int hashCode2 = compactionJob2.hashCode();
       int hashCode3 = compactionJob3.hashCode();
       
       // Then
       assertTrue(equals1);
       assertFalse(equals2);
       assertEquals(hashCode1, hashCode2);
       assertNotEquals(hashCode1, hashCode3);
    }

    @Test
    public void testEqualsAndHashCodeForSplittingJobWithNoIterator() {
       // Given
       CompactionJob compactionJob1 = new CompactionJob("table", "job-1");
       compactionJob1.setInputFiles(Arrays.asList("file1", "file2", "file3"));
       compactionJob1.setOutputFile("outputFile");
       compactionJob1.setIsSplittingJob(true);
       compactionJob1.setPartitionId("partition1");
       compactionJob1.setChildPartitions(Arrays.asList("childPartition1", "childPartition2"));
       compactionJob1.setDimension(2);
       CompactionJob compactionJob2 = new CompactionJob("table", "job-1");
       compactionJob2.setInputFiles(Arrays.asList("file1", "file2", "file3"));
       compactionJob2.setOutputFile("outputFile");
       compactionJob2.setIsSplittingJob(true);
       compactionJob2.setPartitionId("partition1");
       compactionJob2.setChildPartitions(Arrays.asList("childPartition1", "childPartition2"));
       compactionJob2.setDimension(2);
       CompactionJob compactionJob3 = new CompactionJob("table", "job-1");
       compactionJob3.setInputFiles(Arrays.asList("file1", "file2", "file3"));
       compactionJob3.setOutputFile("outputFile1");
       compactionJob3.setIsSplittingJob(true);
       compactionJob3.setPartitionId("partition1");
       compactionJob3.setChildPartitions(Arrays.asList("childPartition2", "childPartition3"));
       compactionJob3.setDimension(2);
       CompactionJob compactionJob4 = new CompactionJob("table", "job-1");
       compactionJob4.setInputFiles(Arrays.asList("file1", "file2", "file3"));
       compactionJob4.setOutputFile("outputFile1");
       compactionJob4.setIsSplittingJob(true);
       compactionJob4.setPartitionId("partition1");
       compactionJob4.setChildPartitions(Arrays.asList("childPartition1", "childPartition2"));
       compactionJob4.setDimension(1);
       
       // When
       boolean equals1 = compactionJob1.equals(compactionJob2);
       boolean equals2 = compactionJob1.equals(compactionJob3);
       boolean equals3 = compactionJob1.equals(compactionJob4);
       int hashCode1 = compactionJob1.hashCode();
       int hashCode2 = compactionJob2.hashCode();
       int hashCode3 = compactionJob3.hashCode();
       int hashCode4 = compactionJob4.hashCode();
       
       // Then
       assertTrue(equals1);
       assertFalse(equals2);
       assertFalse(equals3);
       assertEquals(hashCode1, hashCode2);
       assertNotEquals(hashCode1, hashCode3);
       assertNotEquals(hashCode1, hashCode4);
    }
    
    @Test
    public void testEqualsAndHashCodeForSplittingJobWithIterator() {
       // Given
       CompactionJob compactionJob1 = new CompactionJob("table", "job-1");
       compactionJob1.setInputFiles(Arrays.asList("file1", "file2", "file3"));
       compactionJob1.setOutputFile("outputFile");
       compactionJob1.setIsSplittingJob(true);
       compactionJob1.setPartitionId("partition1");
       compactionJob1.setChildPartitions(Arrays.asList("childPartition1", "childPartition2"));
       compactionJob1.setDimension(2);
       compactionJob1.setIteratorClassName("Iterator.class");
       compactionJob1.setIteratorConfig("config1");
       CompactionJob compactionJob2 = new CompactionJob("table", "job-1");
       compactionJob2.setInputFiles(Arrays.asList("file1", "file2", "file3"));
       compactionJob2.setOutputFile("outputFile");
       compactionJob2.setIsSplittingJob(true);
       compactionJob2.setPartitionId("partition1");
       compactionJob2.setChildPartitions(Arrays.asList("childPartition1", "childPartition2"));
       compactionJob2.setDimension(2);
       compactionJob2.setIteratorClassName("Iterator.class");
       compactionJob2.setIteratorConfig("config1");
       CompactionJob compactionJob3 = new CompactionJob("table", "job-1");
       compactionJob3.setInputFiles(Arrays.asList("file1", "file2", "file3"));
       compactionJob3.setOutputFile("outputFile1");
       compactionJob3.setIsSplittingJob(true);
       compactionJob3.setPartitionId("partition1");
       compactionJob3.setChildPartitions(Arrays.asList("childPartition1", "childPartition2"));
       compactionJob3.setDimension(2);
       compactionJob3.setIteratorClassName("Iterator2.class");
       compactionJob3.setIteratorConfig("config1");
       
       // When
       boolean equals1 = compactionJob1.equals(compactionJob2);
       boolean equals2 = compactionJob1.equals(compactionJob3);
       int hashCode1 = compactionJob1.hashCode();
       int hashCode2 = compactionJob2.hashCode();
       int hashCode3 = compactionJob3.hashCode();
       
       // Then
       assertTrue(equals1);
       assertFalse(equals2);
       assertEquals(hashCode1, hashCode2);
       assertNotEquals(hashCode1, hashCode3);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testShouldThrowOnDuplicateNames() {
       // Given
       CompactionJob job = new CompactionJob("table", "job-1");
       List<String> names = Arrays.asList("file1", "file2", "file3", "file1");
 
       // When
       job.setInputFiles(names);
 
       // Then - throws
    }
 
    @Test(expected = IllegalArgumentException.class)
    public void testShouldThrowOnDuplicateNulls() {
       // Given
       CompactionJob job = new CompactionJob("table", "job-1");
       List<String> names = new ArrayList<>();
       names.add(null);
       names.add(null);
 
       // When
       job.setInputFiles(names);
 
       // Then - throws
    }
}
