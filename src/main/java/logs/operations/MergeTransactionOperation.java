package logs.operations;

import service.SerializationException;
import lombok.Getter;
import service.PrintHelper;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * Merge transaction to the parent segment operation
 */
@Getter
public class MergeTransactionOperation extends  Operation {
    private long transactionSegmentId;
    private long length;


    MergeTransactionOperation(OperationHeader header, DataInputStream source) throws IOException, SerializationException {
        super(header,source);
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        readVersion(source, CURRENT_VERSION);
        setStreamSegmentId(source.readLong());
        this.transactionSegmentId = source.readLong();
        this.length = source.readLong();
        this.streamSegmentOffset = source.readLong();
    }

    @Override
    public void print(){
        super.print();
        PrintHelper.print("TransactionSegmentId",transactionSegmentId,false);
        PrintHelper.print("Length", length,false);
        PrintHelper.print("StreamSegmentOffset",streamSegmentOffset,true);
    }
}
