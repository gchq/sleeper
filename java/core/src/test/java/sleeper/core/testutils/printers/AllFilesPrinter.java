package sleeper.core.testutils.printers;

import sleeper.core.partition.PartitionTree;
import sleeper.core.statestore.AllReferencesToAllFiles;

public class AllFilesPrinter {

    public static String printFiles(PartitionTree tree, AllReferencesToAllFiles files) {
        return FileReferencePrinter.printFiles(tree, files.listFileReferences());
    }

}
