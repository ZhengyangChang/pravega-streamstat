package hdfstool;

import java.io.IOException;

/**
 * Exception that indicates the set of files representing a Segment is corrupted.
 */
public class SegmentFilesCorruptedException extends IOException {
    /**
     * Creates a new instance of the SegmentFilesCorruptedException.
     *
     * @param segmentName The name of the segment.
     * @param file        The file that is out of place.
     * @param message     Custom message.
     */
    public SegmentFilesCorruptedException(String segmentName, FileDescriptor file, String message) {
        super(String.format("Segment '%s' has invalid file '%s'. %s", segmentName, file.getPath(), message));
    }
}
