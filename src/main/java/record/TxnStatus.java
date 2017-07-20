package record;

public enum TxnStatus {
    UNKNOWN,
    OPEN,
    COMMITTING,
    COMMITTED,
    ABORTING,
    ABORTED
}
