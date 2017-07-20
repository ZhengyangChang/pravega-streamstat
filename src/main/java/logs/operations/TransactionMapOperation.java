package logs.operations;

import service.SerializationException;
import lombok.Getter;
import service.PrintHelper;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Log Operation that represents a mapping between a Transaction StreamSegment and its Parent StreamSegment.
 */
public class TransactionMapOperation extends Operation {
    @Getter
    private long parentStreamSegmentId;

    private String streamSegmentName;
    private long length;
    private boolean sealed;
    private Map<UUID, Long> attributes;

    TransactionMapOperation(Operation.OperationHeader header, DataInputStream source) throws IOException, SerializationException {
        super(header,source);
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        readVersion(source, CURRENT_VERSION);
        this.parentStreamSegmentId = source.readLong();
        this.streamSegmentId = source.readLong();
        this.streamSegmentName = source.readUTF();
        this.length = source.readLong();
        this.sealed = source.readBoolean();
        this.attributes = AttributeSerializer.deserialize(source);
    }

    @Override
    public void print(){
        super.print();
        PrintHelper.print("ParentStreamSegmentId",parentStreamSegmentId,false);
        PrintHelper.print("StreamSegmentName",streamSegmentName,false);
        PrintHelper.print("Length",length,false);
        PrintHelper.print("Sealed",sealed,false);
        PrintHelper.print(attributes);

    }
}
