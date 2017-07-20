package logs.operations;

import service.SerializationException;
import io.pravega.common.io.StreamHelpers;
import lombok.Getter;
import service.PrintHelper;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;

/**
 * Append operation
 */
public class StreamSegmentAppendOperation extends Operation{
    private Collection<AttributeUpdate> attributeUpdates;

    @Getter
    private byte[] data;
    StreamSegmentAppendOperation(OperationHeader header, DataInputStream source) throws IOException,SerializationException{
        super(header,source);
    }

    void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        readVersion(source, CURRENT_VERSION);
        setStreamSegmentId(source.readLong());
        setStreamSegmentOffset(source.readLong());
        int dataLength = source.readInt();
        this.data = new byte[dataLength];
        int bytesRead = StreamHelpers.readAll(source, this.data, 0, this.data.length);
        assert bytesRead == this.data.length : "StreamHelpers.readAll did not read all the bytes requested.";
        this.attributeUpdates = AttributeSerializer.deserializeUpdates(source);
    }

    @Override
    public void print(){
        super.print();
        PrintHelper.print("StreamSegmentOffset",streamSegmentOffset,false);
        PrintHelper.print("AttributeUpdate",attributeUpdates.toString(), true);

    }
}
