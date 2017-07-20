package record;

import io.pravega.common.util.BitConverter;
import lombok.Data;
import service.PrintHelper;

import java.util.Date;

/**
 * Active Transaction Record
 */
@Data
public class ActiveTxnRecord {
    private static final int ACTIVE_TXN_RECORD_SIZE = 4 * Long.BYTES + Integer.BYTES;
    private final long txCreationTimestamp;
    private final long leaseExpiryTime;
    private final long maxExecutionExpiryTime;
    private final long scaleGracePeriod;
    private final TxnStatus txnStatus;
    private final int segment;

    public static ActiveTxnRecord parse(final byte[] bytes, int epoch) {
        final int longSize = Long.BYTES;

        final long txCreationTimestamp = BitConverter.readLong(bytes, 0);

        final long leaseExpiryTime = BitConverter.readLong(bytes, longSize);

        final long maxExecutionExpiryTime = BitConverter.readLong(bytes, 2 * longSize);

        final long scaleGracePeriod = BitConverter.readLong(bytes, 3 * longSize);

        final TxnStatus status = TxnStatus.values()[BitConverter.readInt(bytes, 4 * longSize)];

        return new ActiveTxnRecord(txCreationTimestamp, leaseExpiryTime, maxExecutionExpiryTime, scaleGracePeriod, status, epoch);
    }

    public void print(){
        PrintHelper.print("txCreationTimestamp", new Date(txCreationTimestamp), false);
        PrintHelper.print("leaseExpiryTime", new Date(leaseExpiryTime), false);
        PrintHelper.print("maxExecutionExpiryTime", new Date(maxExecutionExpiryTime), false);
        PrintHelper.print("scaleGracePeriod", scaleGracePeriod, false);
        PrintHelper.print("txnStatus", txnStatus, false);
        PrintHelper.print("epoch", segment, true);
    }
}