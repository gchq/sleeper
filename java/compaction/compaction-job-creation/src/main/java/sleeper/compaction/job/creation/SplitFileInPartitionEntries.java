package sleeper.compaction.job.creation;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.schema.Schema;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

public class SplitFileInPartitionEntries {
    private static final Logger LOGGER = LoggerFactory.getLogger(SplitFileInPartitionEntries.class);

    private final Schema schema;
    private final List<FileInfo> fileInPartitionEntries;
    private final List<Partition> allPartitions;
    private final StateStore stateStore;

    public SplitFileInPartitionEntries(Schema schema,
            List<FileInfo> fileInPartitionEntries,
            List<Partition> allPartitions,
            StateStore stateStore) {
        this.schema = schema;
        this.fileInPartitionEntries = fileInPartitionEntries;
        this.allPartitions = allPartitions;
        this.stateStore = stateStore;
    }
    
    public void run() throws StateStoreException {
        PartitionTree partitionTree = new PartitionTree(schema, allPartitions);
        for (FileInfo fileInfo : fileInPartitionEntries) {
            String partitionId = fileInfo.getPartitionId();
            Partition partition = partitionTree.getPartition(partitionId);
            if (!partition.isLeafPartition()) {
                LOGGER.info("File-in-partition entry for file {} and partition {} is being split as it is not in a leaf partition",
                    fileInfo.getFilename(), partitionId);
                List<String> childPartitionIds = partition.getChildPartitionIds();
                stateStore.atomicallySplitFileInPartitionRecord(fileInfo, childPartitionIds.get(0), childPartitionIds.get(1));
            }
        }
    }
}
