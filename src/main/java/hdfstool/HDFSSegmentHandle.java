package hdfstool;

import io.pravega.common.Exceptions;

import java.nio.file.FileSystem;
import java.util.List;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.apache.http.annotation.GuardedBy;
import org.apache.http.annotation.ThreadSafe;
import service.ExceptionHandler;
import service.PrintHelper;

/**
 * Base Handle for HDFSStorage.
 */
@ThreadSafe
public class HDFSSegmentHandle {
    //region Members

    @Getter
    private final String segmentName;
    @Getter
    private final boolean readOnly;
    @GuardedBy("files")
    @Getter
    private final List<FileDescriptor> files;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the HDFSSegmentHandle class.
     *
     * @param segmentName The name of the Segment in this Handle, as perceived by users of the Segment interface.
     * @param readOnly    Whether this handle is read-only or not.
     * @param files       A ordered list of initial files for this handle.
     */
    private HDFSSegmentHandle(String segmentName, boolean readOnly, List<FileDescriptor> files) {
        Exceptions.checkNotNullOrEmpty(segmentName, "segmentName");
        Exceptions.checkNotNullOrEmpty(files, "files");

        this.segmentName = segmentName;
        this.readOnly = readOnly;
        this.files = files;
    }

    /**
     * Creates a read-only handle.
     *
     * @param segmentName The name of the Segment to create the handle for.
     * @param files       A ordered list of initial files for this handle.
     * @return The new handle.
     */
    public static HDFSSegmentHandle read(String segmentName, List<FileDescriptor> files) {
        return new HDFSSegmentHandle(segmentName, true, files);
    }

    //endregion

    //region print

    public void print(){
        PrintHelper.printHead("HDFS Storage segment");
        PrintHelper.println();
        PrintHelper.print("Storage Epoch", getEpoch(), false);

        String fileNames;
        synchronized (this.files) {
            fileNames = StringUtils.join(this.files, ", ");
        }

        PrintHelper.print(String.format("(Files: %s)", fileNames));

        PrintHelper.println();
        PrintHelper.println();
    }

    private long getEpoch(){
        return getLastFile().getEpoch();
    }

    /**
     * Gets the last file in the file list for this handle.
     */
    private FileDescriptor getLastFile() {
        synchronized (this.files) {
            return this.files.get(this.files.size() - 1);
        }
    }

    /**
     * Get the total length
     */
    public long getLength(){
        return files.stream().mapToLong(FileDescriptor::getLength).sum();
    }

    //endregion

    public void checkInvariants(){
        long start = Long.MAX_VALUE, end = 0;

        for (FileDescriptor fd:
                files){
            start = fd.getOffset() < start ? fd.getOffset() : start;
            end = fd.getLastOffset() > end ? fd.getLastOffset() : end;
        }

        if (start != 0 )
            ExceptionHandler.T2_OFFSET_NOT_START_ZERO.apply();

        if (end - start != getLength())
            ExceptionHandler.DATA_NOT_CONTINUOUS.apply();
    }
}
