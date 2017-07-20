package hdfstool;

import io.pravega.common.util.CollectionHelpers;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An operation for hdfs read
 */
@RequiredArgsConstructor
public class HDFSRead {
    private final long offset;

    @Getter
    private final byte[] buffer;

    private final int length;
    private final FileSystem fs;


    public void read(HDFSSegmentHandle handle, AtomicInteger totalBytesRead) throws IOException {
        val handleFiles = handle.getFiles();
        int currentFileIndex = CollectionHelpers.binarySearch(handleFiles, this::compareToStartOffset);
        assert currentFileIndex >= 0 : "unable to locate first file index.";
        while (totalBytesRead.get() < this.length && currentFileIndex < handleFiles.size()) {
            FileDescriptor currentFile = handleFiles.get(currentFileIndex);
            long fileOffset = this.offset + totalBytesRead.get() - currentFile.getOffset();
            int fileReadLength = (int) Math.min(this.length - totalBytesRead.get(), currentFile.getLength() - fileOffset);
            assert fileOffset >= 0 && fileReadLength >= 0 : "negative file read offset or length";

            try (FSDataInputStream stream = this.fs.open(currentFile.getPath())) {
                stream.readFully(fileOffset, this.buffer, totalBytesRead.get(), fileReadLength);
                totalBytesRead.addAndGet(fileReadLength);
            } catch (EOFException ex) {
                throw new IOException(
                        String.format("Internal error while reading segment file. Attempted to read file '%s' at offset %d, length %d.",
                                currentFile, fileOffset, fileReadLength),
                        ex);
            }

            currentFileIndex++;
        }
    }

    private int compareToStartOffset(FileDescriptor fi) {
        if (this.offset < fi.getOffset()) {
            return -1;
        } else if (this.offset >= fi.getLastOffset()) {
            return 1;
        } else {
            return 0;
        }
    }

}
