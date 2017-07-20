package record;

import lombok.Data;

/**
 * Stores the event data and its info
 */
@Data
public class DataRecord {
    final String toPrint;
    final int events;
    final long length;
}
