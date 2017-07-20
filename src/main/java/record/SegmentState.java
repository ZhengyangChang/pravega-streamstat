package record;

import lombok.Data;
import service.PrintHelper;

import java.util.Map;
import java.util.UUID;

/**
 * Segment states (stored in zookeeper)
 */
@Data
public class SegmentState {
    private final byte version;
    private final long segmentId;
    private final String segmentName;
    private final Map<UUID, Long> attributes;

    public void print(){
        PrintHelper.printHead("Segment State");
        PrintHelper.print("SegmentId",segmentId == Long.MIN_VALUE ? "MISSING" : String.format("%d",segmentId) ,false);
        PrintHelper.print("SegmentName", segmentName, false);
        PrintHelper.print(attributes);
    }
}
