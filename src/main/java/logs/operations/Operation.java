package logs.operations;

import service.SerializationException;
import lombok.Data;
import service.PrintHelper;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Operations
 */
@Data
public abstract class Operation {
    static final byte CURRENT_VERSION = 0;

    OperationHeader header;
    byte version;
    long streamSegmentId;
    long streamSegmentOffset;


    public static Operation deserialize(InputStream input) throws SerializationException {
        DataInputStream source = new DataInputStream(input);
        try {
            OperationHeader header = new OperationHeader(source);
            OperationType type = OperationType.getOperationType(header.getType());
            if (type == OperationType.Probe)
                throw new SerializationException("Operation.deserialize", "Invalid operation type");
            return type.deserializationConstructor.apply(header,source);
        } catch (IOException e) {
            throw new SerializationException("Operation.deserialize", "Unable to deserialize");
        }

    }

    Operation(OperationHeader header, DataInputStream source) throws IOException, SerializationException {
        this.header = header;
        deserializeContent(source);
    }

    abstract void deserializeContent(DataInputStream source) throws IOException, SerializationException;


    /**
     * Reads a version byte from the given input stream and compares it to the given expected version.
     *
     * @param source          The input stream to read from.
     * @param expectedVersion The expected version to compare to.
     * @throws IOException            If the input stream threw one.
     * @throws SerializationException If the versions mismatched.
     */
    void readVersion(DataInputStream source, byte expectedVersion) throws IOException, SerializationException {
        version = source.readByte();
        if (version != expectedVersion) {
            throw new SerializationException(String.format("%s.deserialize", this.getClass().getSimpleName()), String.format("Unsupported version: %d.", version));
        }
    }





    @Data
    public static class OperationHeader {
        final byte version;
        final byte type;
        final long sequenceNumber;

        OperationHeader(DataInputStream source) throws IOException {
            version = source.readByte();
            type = source.readByte();
            sequenceNumber = source.readLong();
        }
    }


    public void print() {

        PrintHelper.printHead("Operation");
        PrintHelper.print("Type", OperationType.getOperationType(header.getType()).toString(), false);
        PrintHelper.print("Sequence",header.getSequenceNumber(),false);
        PrintHelper.print("StreamSegmentId",streamSegmentId,false);

    }
}


