package logs.operations;

import service.SerializationException;
import service.PrintHelper;

import java.io.DataInputStream;
import java.io.IOException;

/**
 *  Log Operation that indicates a StreamSegment has been sealed.
 */
public class StreamSegmentSealOperation extends Operation{
    StreamSegmentSealOperation(Operation.OperationHeader header, DataInputStream source) throws IOException, SerializationException {
        super(header,source);
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        readVersion(source, CURRENT_VERSION);
        setStreamSegmentId(source.readLong());
        this.streamSegmentOffset = source.readLong();
    }

    @Override
    public void print(){
        super.print();
        PrintHelper.print("StreamSegmentOffset",streamSegmentOffset,true);
    }
}
