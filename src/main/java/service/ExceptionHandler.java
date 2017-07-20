package service;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * exception handlers
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum ExceptionHandler {

    // region input errors
    INVALID_STREAM_NAME("INVALID STREAM NAME", false),
    CANNOT_READ_STREAM_NAME("CANNOT READ STREAM NAME", false),
    // endregion

    // region HDFS errors
    HDFS_CANNOT_CONNECT("Cannot connect to HDFS, please check the HDFS url given.", true),

    // endregion

    // region zk errors
    NO_STREAM("No such stream", false),
    NO_CLUSTER("Cannot find pravega cluster in zookeeper, please check zookeeper url.", false),

    // endregion

    // region invariants not hold.
    NO_SEGMENT_ID("Cannot find segment ID in both tier.",true),
    NO_TXN("NO transaction found.", true),
    DATA_NOT_CONTINUOUS("Data is not consistent in tier 1 log, some log might be lost", true),
    T2_OFFSET_NOT_START_ZERO("Tier-2 offset not start with zero", true),
    T1_OFFSET_LOWER_THAN_T2("Tier-1 largest offset is smaller than tier-2", true),
    // endregion

    ;
    final String msg;
    final boolean isWarning;

    static boolean blockOnWarning = false;
    private static List<ExceptionHandler> accumulator = new ArrayList<>();
    public void apply(){
        PrintHelper.printError(msg);
        if (!isWarning || blockOnWarning){
            System.exit(-1);
        }

        accumulator.add(this);
    }

    public static void summarize(){
        if (accumulator.isEmpty()) {
            PrintHelper.println(PrintHelper.Color.GREEN, "No errors found");
            return;
        }

        PrintHelper.printHead("These issues are found");
        for (int i=0; i < accumulator.size(); i++ ) {
            ExceptionHandler e = accumulator.get(i);
            PrintHelper.print(e.toString(),e.msg,i == accumulator.size()-1);
        }
    }
}
