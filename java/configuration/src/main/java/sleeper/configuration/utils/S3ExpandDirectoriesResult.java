package sleeper.configuration.utils;

import java.util.List;
import java.util.stream.Stream;

/**
 * Results of expanding directories in S3.
 *
 * @param paths the paths that were expanded and their contents
 */
public record S3ExpandDirectoriesResult(List<S3PathContents> paths) {

    public List<String> listHadoopPathsThrowIfAnyPathIsEmpty() {
        return streamFilesThrowIfAnyPathIsEmpty()
                .map(S3FileDetails::pathForHadoop)
                .toList();
    }

    public List<String> listJobPaths() {
        return streamFiles()
                .map(S3FileDetails::pathForJob)
                .toList();
    }

    public List<String> listMissingPaths() {
        return paths.stream()
                .filter(S3PathContents::isEmpty)
                .map(S3PathContents::requestedPath)
                .toList();
    }

    public Stream<S3FileDetails> streamFilesThrowIfAnyPathIsEmpty() {
        return paths.stream()
                .peek(S3PathContents::throwIfEmpty)
                .flatMap(path -> path.files().stream());
    }

    public Stream<S3FileDetails> streamFiles() {
        return paths.stream()
                .flatMap(path -> path.files().stream());
    }
}