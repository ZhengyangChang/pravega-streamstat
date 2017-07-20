package bookkeepertool;

import lombok.Data;

/**
 * mock io.pravega.segmentstore.storage.impl.bookkeeper.BookKeepers.LedgerMetadata for test
 */
@Data
public class LedgerMetadata {
    /**
     * The BookKeeper-assigned Ledger Id.
     */
    private final long ledgerId;

    /**
     * The metadata-assigned internal sequence number of the Ledger inside the log.
     */
    private final int sequence;

    @Override
    public String toString() {
        return String.format("{Id = %d, Sequence = %d}", this.ledgerId, this.sequence);
    }
}
