package logs.operations;

import service.SerializationException;
import lombok.Getter;
import service.PrintHelper;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Log Operation that represents a mapping of StreamSegment Name to a StreamSegment Id.
 */
@Getter
public class StreamSegmentMapOperation extends Operation{
    private String streamSegmentName;
    private long length;
    private boolean sealed;
    private Map<UUID, Long> attributes;

    StreamSegmentMapOperation(Operation.OperationHeader header, DataInputStream source) throws IOException, SerializationException {
        super(header,source);
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        readVersion(source, CURRENT_VERSION);
        this.streamSegmentId = source.readLong();
        this.streamSegmentName = source.readUTF();
        this.length = source.readLong();
        this.sealed = source.readBoolean();
        this.attributes = AttributeSerializer.deserialize(source);
    }

    @Override
    public void print(){
        super.print();
        PrintHelper.print("StreamSegmentName",streamSegmentName,false);
        PrintHelper.print("Length",length, false);
        PrintHelper.print("Sealed",sealed,false);
        PrintHelper.print(attributes);
    }


}
