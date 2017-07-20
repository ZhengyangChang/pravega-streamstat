package logs.operations;

import service.SerializationException;
import io.pravega.common.util.ByteArraySegment;
import service.PrintHelper;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * Store a checkpoint
 */
public class StorageMetadataCheckpointOperation extends Operation {
    private ByteArraySegment contents;

    StorageMetadataCheckpointOperation(Operation.OperationHeader header, DataInputStream source) throws IOException, SerializationException {
        super(header,source);
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        readVersion(source, CURRENT_VERSION);
        int contentsLength = source.readInt();
        this.contents = new ByteArraySegment(new byte[contentsLength]);
        int bytesRead = this.contents.readFrom(source);
        assert bytesRead == contentsLength : "StreamHelpers.readAll did not read all the bytes requested.";
    }

    @Override
    public void print(){
        super.print();
        PrintHelper.print("MetadataLength", contents.getLength(),true);
    }
}
