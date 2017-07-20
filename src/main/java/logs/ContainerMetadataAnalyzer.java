package logs;

import service.SerializationException;
import logs.operations.AttributeSerializer;
import logs.operations.MetadataCheckpointOperation;
import io.pravega.common.util.ImmutableDate;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.val;
import service.PrintHelper;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

/**
 * Meta data fetcher
 */
@RequiredArgsConstructor
public class ContainerMetadataAnalyzer {
    // region Members

    private static final byte CURRENT_SERIALIZATION_VERSION = 0;

    private final int containerId;
    private final String targetSegmentName;
    private SegmentMetadata targetMetadata;

    @Setter
    private long knownId = -1;
    // endregion

    /**
     * Deserialize the Metadata from the given stream.
     *
     * @param operation The MetadataCheckpointOperation to deserialize from.
     * @throws IOException            If the stream threw one.
     * @throws SerializationException If the given Stream is an invalid metadata serialization.
     * @throws IllegalStateException  If the Metadata is not in Recovery Mode.
     */
    public SegmentMetadata deserializeFrom(MetadataCheckpointOperation operation) throws IOException, SerializationException {

        DataInputStream stream = new DataInputStream(new GZIPInputStream(operation.getContents().getReader()));

        // 1. Version.
        byte version = stream.readByte();
        if (version != CURRENT_SERIALIZATION_VERSION) {
            throw new SerializationException("Metadata.deserialize", String.format("Unsupported version: %d.", version));
        }

        // 2. Container id.
        int containerId = stream.readInt();
        if (this.containerId != containerId) {
            throw new SerializationException("Metadata.deserialize",
                    String.format("Invalid ContainerId. Expected '%d', actual '%d'.", this.containerId, containerId));
        }

        // 3. Stream Segments (unchanged).
        int segmentCount = stream.readInt();
        for (int i = 0; i < segmentCount; i++) {
            deserializeSegmentMetadata(stream);
        }

        // 4. Stream Segments (updated).
        segmentCount = stream.readInt();
        for (int i = 0; i < segmentCount; i++) {
            deserializeSegmentMetadata(stream);
        }

        // 5. New Stream Segments.
        segmentCount = stream.readInt();
        for (int i = 0; i < segmentCount; i++) {
            deserializeSegmentMetadata(stream);
        }

        return targetMetadata;
    }

    private void deserializeSegmentMetadata(DataInputStream stream) throws IOException {
        // S1. SegmentId.
        long segmentId = stream.readLong();
        // S2. ParentId.
        long parentId = stream.readLong();
        // S3. Name.
        String name = stream.readUTF();

        SegmentMetadata metadata = new SegmentMetadata(segmentId,parentId,name);
        // S4. DurableLogLength.
        metadata.setDurableLogLength(stream.readLong());
        // S5. StorageLength.
        metadata.setStorageLength(stream.readLong());
        // S6. Merged.
        boolean isMerged = stream.readBoolean();
        if (isMerged) {
            metadata.setMerged(true);
        }
        // S7. Sealed.
        boolean isSealed = stream.readBoolean();
        if (isSealed) {
            metadata.setSealed(true);
        }
        // S8. SealedInStorage.
        boolean isSealedInStorage = stream.readBoolean();
        if (isSealedInStorage) {
            metadata.setSealedInStorage(true);
        }
        // S9. Deleted.
        boolean isDeleted = stream.readBoolean();
        if (isDeleted) {
            metadata.setDeleted(true);
        }
        // S10. LastModified.
        ImmutableDate lastModified = new ImmutableDate(stream.readLong());
        metadata.setLastModified(lastModified);

        // S11. Attributes.
        val attributes = AttributeSerializer.deserialize(stream);
        metadata.setAttributeValues(attributes);


        if (metadata.name.equals(targetSegmentName)) {
            targetMetadata = metadata;
            knownId = segmentId;
        }

        if (metadata.parentId == knownId){
            PrintHelper.printHead("Transaction segment: ");
            metadata.print();
        }
    }


    public void print(){
        if (targetMetadata == null)
            return;
        PrintHelper.printHead("Segment Metadata: ");

        targetMetadata.print();

    }

    @RequiredArgsConstructor
    @Setter
    public static class SegmentMetadata{
        @Getter
        final long segmentId;
        final long parentId;
        final String name;

        private long durableLogLength;
        private long storageLength;
        private boolean merged;
        private boolean sealed;
        private boolean sealedInStorage;
        private boolean deleted;
        private ImmutableDate lastModified;
        private Map<UUID, Long> attributeValues;

        private void print(){
            PrintHelper.print("SegmentId", segmentId, false);
            PrintHelper.print("ParentId", parentId == Long.MIN_VALUE ? "NONE" : parentId, false);
            PrintHelper.print("Name", name, false);
            PrintHelper.print("DurableLogLength", durableLogLength, false);
            PrintHelper.print("StorageLength", storageLength, false);
            PrintHelper.print("Merged", merged, false);
            PrintHelper.print("Sealed", sealed, false);
            PrintHelper.print("SealedInStorage", sealedInStorage, false);
            PrintHelper.print("Deleted", deleted, false);
            PrintHelper.print("LastModified", new Date(lastModified.getTime()), false);
            PrintHelper.print(attributeValues);
        }
    }
}
