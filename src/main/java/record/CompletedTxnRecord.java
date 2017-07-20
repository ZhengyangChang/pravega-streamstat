package record;

import io.pravega.common.util.BitConverter;
import lombok.Data;
import service.PrintHelper;

import java.util.Date;

@Data
public class CompletedTxnRecord {
    private static final int COMPLETED_TXN_RECORD_SIZE = Long.BYTES + Integer.BYTES;

    private final long completeTime;
    private final TxnStatus completionStatus;

    public static CompletedTxnRecord parse(final byte[] bytes) {
        final long completeTimeStamp = BitConverter.readLong(bytes, 0);

        final TxnStatus status = TxnStatus.values()[BitConverter.readInt(bytes, Long.BYTES)];

        return new CompletedTxnRecord(completeTimeStamp, status);
    }

    public void print(){
        PrintHelper.print("completeTime",new Date(completeTime),false);
        PrintHelper.print("completionStatus",completionStatus,true);
    }
}
