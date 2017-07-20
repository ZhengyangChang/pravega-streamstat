package service;

import bookkeepertool.LogMetadata;
import bookkeepertool.LogReader;
import logs.ContainerMetadataAnalyzer;
import logs.operations.MergeTransactionOperation;
import logs.operations.MetadataCheckpointOperation;
import logs.operations.Operation;
import logs.operations.StreamSegmentAppendOperation;
import logs.operations.StreamSegmentMapOperation;
import logs.operations.TransactionMapOperation;
import io.pravega.common.util.ByteArraySegment;
import lombok.RequiredArgsConstructor;
import org.apache.bookkeeper.client.BookKeeper;
import record.DataRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Functions for analysing operation log.
 */
@RequiredArgsConstructor
class OperationAnalyzer {
    private final BookKeeper bkClient;
    private final String segmentName;
    private final LogMetadata log;
    private final Map<String,Long> segmentNameToIdMap;
    private final StreamStat.StreamStatConfigure conf;

    private long totalBytesOfSegment;
    private int totalEventsOfSegment;
    // txn segment id to event number map.
    private Map<Long,Integer> transactionSegments = new HashMap<>();
    private long offsetStart;
    private long currentOffset;
    private boolean dataContinuity;

    private void init(){
        totalBytesOfSegment = 0;
        totalEventsOfSegment = 0;
        transactionSegments = new HashMap<>();
        offsetStart = -1;
        currentOffset = 0;
        dataContinuity = true;
    }
    void printLog() throws SerializationException {

        LogReader reader = new LogReader(bkClient , log, conf.fence, 0);
        if (!conf.log || conf.explicit) PrintHelper.block();
        printLogWithReader(reader);
        PrintHelper.unblock();

        if (!conf.simple) return;

        if (conf.explicit) redo();

        if (!dataContinuity)
            ExceptionHandler.DATA_NOT_CONTINUOUS.apply();
        if (currentOffset <
                HDFSHelper.segmentLength.get(segmentName))
            ExceptionHandler.T1_OFFSET_LOWER_THAN_T2.apply();



        PrintHelper.print("Total Bytes in T1 log", totalBytesOfSegment, false);
        PrintHelper.print("Total Events in T1 log", totalEventsOfSegment, false);


        PrintHelper.print("Offset start", offsetStart < 0 ? 0: offsetStart, false);
        PrintHelper.print("Offset end", currentOffset, true);





    }

    private void printLogWithReader(LogReader reader) throws SerializationException{
        init();
        LogReader.ReadItem item = reader.getNext();

        while (item != null){
            List<ByteArraySegment> segments = BKHelper.getSegments(item,conf.simple);

            Operation operation = BKHelper.getOperation(segments);
            printOperation(operation);
            item = reader.getNext();
        }


    }
    /**
     * Print the operation according to current segment name
     * @param operation the operation to print
     * @throws SerializationException throw when cannot deserialize data.
     */
    private void printOperation(Operation operation) throws SerializationException{

        if (conf.simple) {
            if (operation instanceof MetadataCheckpointOperation){
                ContainerMetadataAnalyzer metadataUpdateTransaction =
                        new ContainerMetadataAnalyzer(
                                log.getContainerId(), segmentName);

                if (segmentNameToIdMap.containsKey(segmentName))
                    metadataUpdateTransaction.setKnownId(segmentNameToIdMap.get(segmentName));

                try {
                    ContainerMetadataAnalyzer.SegmentMetadata segmentMetadata =
                            metadataUpdateTransaction.deserializeFrom((MetadataCheckpointOperation) operation);
                    if (segmentMetadata != null) {
                        segmentNameToIdMap.put(segmentName, segmentMetadata.getSegmentId());
                        metadataUpdateTransaction.print();
                    }

                } catch (IOException e) {
                    throw new SerializationException("MetadataCheckPointOperation","Unable to read");
                }

            }
            if (operation instanceof StreamSegmentMapOperation){
                if (segmentName.equals(((StreamSegmentMapOperation) operation).getStreamSegmentName())){
                    segmentNameToIdMap.put(segmentName, operation.getStreamSegmentId());
                    operation.print();
                }
            } else if (operation instanceof TransactionMapOperation) {
                Long segmentId = segmentNameToIdMap.get(segmentName);

                if (segmentId != null &&
                        segmentId.equals(
                                ((TransactionMapOperation) operation)
                                        .getParentStreamSegmentId())) {
                    transactionSegments.put(operation.getStreamSegmentId(), 0);
                    operation.print();
                }
            }
            else {
                Long segmentId = segmentNameToIdMap.get(segmentName);

                if (segmentId != null &&
                        (operation.getStreamSegmentId() == segmentId ||
                                transactionSegments.keySet().contains(operation.getStreamSegmentId()))){
                    operation.print();
                    processOperation(operation);

                }
            }
        }
        else{
            operation.print();
            processOperation(operation);
        }
    }

    /**
     * Print Data of the append operation and add statistics about the operation
     * @param operation the operation to print
     * @throws SerializationException throw when cannot serialize data
     */
    private void processOperation(Operation operation) throws SerializationException{
        if (operation instanceof StreamSegmentAppendOperation){
            byte[] data = ((StreamSegmentAppendOperation) operation).getData();

            DataRecord dataRecord = EventDataAnalyzer.getDataRecord(data,conf.data);

            if (dataRecord.getLength() != data.length){
                throw new SerializationException("Append Operation Data", "Data not consistent, some byte maybe lost.");
            }

            PrintHelper.println(PrintHelper.Color.PURPLE, dataRecord.getToPrint());
            PrintHelper.println();

            updateStat(operation.getStreamSegmentOffset(),operation.getStreamSegmentId(),
                    dataRecord.getLength(), dataRecord.getEvents());

        }

        if (conf.simple && operation instanceof MergeTransactionOperation){
            long transactionSegmentId = ((MergeTransactionOperation) operation).getTransactionSegmentId();
            int events = transactionSegments.get(transactionSegmentId);

            updateStat(operation.getStreamSegmentOffset(),operation.getStreamSegmentId(),
                    ((MergeTransactionOperation) operation).getLength(), events);
        }
    }

    /**
     * Update the statistic value (bytes and event).
     * @param offset current operation offset.
     * @param operationSegmentId operations segment id (used to check origin segment or txn segment).
     * @param length length of the operation data.
     * @param events number of events in operation.
     */
    private void updateStat(long offset, long operationSegmentId, long length, int events){

        Long segmentId = segmentNameToIdMap.get(segmentName);

        if (segmentId != null &&
                operationSegmentId != segmentId) {
            int currentTxEvents = transactionSegments.get(operationSegmentId);

            transactionSegments.put(operationSegmentId, events + currentTxEvents);

        } else {

            totalBytesOfSegment += length;
            totalEventsOfSegment += events;

            if (offsetStart < 0) {
                offsetStart = offset;
                currentOffset = offsetStart;
            }

            if (currentOffset != offset)
                dataContinuity = false;

            currentOffset += length;
        }
    }

    /**
     * reprint only if there is data missing
     */
    private void redo() throws SerializationException{
        LogReader reader;
        if (!dataContinuity ||
                currentOffset <
                        HDFSHelper.segmentLength.get(segmentName)) {
            PrintHelper.println();
            PrintHelper.println(PrintHelper.Color.RED, "Some log may have not been read, use long polling read to read explicit logs");
            PrintHelper.println();

            reader = new LogReader(bkClient, log, conf.fence, 1);
        }
        else
            reader = new LogReader(bkClient, log, conf.fence, 0);
        if (!conf.log) PrintHelper.block();
        printLogWithReader(reader);
        PrintHelper.unblock();

    }

}
