package logs.operations;

import service.SerializationException;
import service.PrintHelper;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;

/**
 * Log Operation that represents an Update to a Segment's Attribute collection.
 */
public class UpdateAttributesOperation extends Operation {
    private Collection<AttributeUpdate> attributeUpdates;

    UpdateAttributesOperation(OperationHeader header, DataInputStream source) throws IOException, SerializationException {
        super(header,source);
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        readVersion(source, CURRENT_VERSION);
        this.streamSegmentId = source.readLong();
        this.attributeUpdates = AttributeSerializer.deserializeUpdates(source);
    }

    @Override
    public void print(){
        super.print();

        PrintHelper.print("AttributeUpdates",attributeUpdates.toString(),true);
    }
}
