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
package sleeper.statestore;

import sleeper.core.key.Key;
import sleeper.core.schema.type.PrimitiveType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Stores metadata about a file such as its filename, which partition it is in,
 * its status (e.g. active, ready for garbage collection), the min and max
 * values in the file, and optionally a job id indicating which compaction
 * job is responsible for compacting it.
 */
public class FileInfo {
    public enum FileStatus {
        ACTIVE, READY_FOR_GARBAGE_COLLECTION
    }

    private List<PrimitiveType> rowKeyTypes;
    private Key minRowKey;
    private Key maxRowKey;
    private String filename;
    private String partitionId;
    private Long numberOfRecords;
    private FileStatus fileStatus;
    private String jobId;
    private Long lastStateStoreUpdateTime; // The latest time (in milliseconds since the epoch) that the status of the file was updated in the StateStore

    public FileInfo() {

    }

    public List<PrimitiveType> getRowKeyTypes() {
        return rowKeyTypes;
    }

    public void setRowKeyTypes(List<PrimitiveType> rowKeyTypes) {
        this.rowKeyTypes = rowKeyTypes;
    }

    public void setRowKeyTypes(PrimitiveType... rowKeyTypes) {
        this.rowKeyTypes = new ArrayList<>();
        for (PrimitiveType type : rowKeyTypes) {
            this.rowKeyTypes.add(type);
        }
    }
    
    public Key getMinRowKey() {
        return minRowKey;
    }

    public void setMinRowKey(Key minRowKey) {
        this.minRowKey = minRowKey;
    }
    
    public Key getMaxRowKey() {
        return maxRowKey;
    }

    public void setMaxRowKey(Key maxRowKey) {
        this.maxRowKey = maxRowKey;
    }
    
    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getFilename() {
        return filename;
    }

    public void setFileStatus(FileStatus fileStatus) {
        this.fileStatus = fileStatus;
    }

    public FileStatus getFileStatus() {
        return fileStatus;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJobId() {
        return jobId;
    }

    public void setLastStateStoreUpdateTime(Long lastStateStoreUpdateTime) {
        this.lastStateStoreUpdateTime = lastStateStoreUpdateTime;
    }

    public Long getLastStateStoreUpdateTime() {
        return lastStateStoreUpdateTime;
    }

    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public void setNumberOfRecords(Long numberOfRecords) {
        this.numberOfRecords = numberOfRecords;
    }

    public Long getNumberOfRecords() {
        return numberOfRecords;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileInfo fileInfo = (FileInfo) o;
        
        return Objects.equals(rowKeyTypes, fileInfo.rowKeyTypes) &&
                Objects.equals(filename, fileInfo.filename) &&
                Objects.equals(partitionId, fileInfo.partitionId) &&
                Objects.equals(numberOfRecords, fileInfo.numberOfRecords) &&
                Objects.equals(minRowKey, fileInfo.minRowKey) &&
                Objects.equals(maxRowKey, fileInfo.maxRowKey) &&
                fileStatus == fileInfo.fileStatus &&
                Objects.equals(jobId, fileInfo.jobId) &&
                Objects.equals(lastStateStoreUpdateTime, fileInfo.lastStateStoreUpdateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowKeyTypes, filename, partitionId,
                numberOfRecords, minRowKey, maxRowKey,
                fileStatus, jobId, lastStateStoreUpdateTime);
    }

    @Override
    public String toString() {
        return "FileInfo{" +
                "rowKeyTypes=" + rowKeyTypes + 
                ", filename='" + filename + '\'' +
                ", partition='" + partitionId + '\'' +
                ", numberOfRecords=" + numberOfRecords +
                ", minKey=" + minRowKey +
                ", maxKey=" + maxRowKey +
                ", fileStatus=" + fileStatus +
                ", jobId='" + jobId + '\'' +
                ", lastStateStoreUpdateTime=" + lastStateStoreUpdateTime +
                '}';
    }
}
