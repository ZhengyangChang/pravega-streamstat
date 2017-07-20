package service;

import logs.DataFrame;
import bookkeepertool.LogReader;
import logs.operations.Operation;
import com.google.common.collect.Iterators;
import io.pravega.common.util.ByteArraySegment;

import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Utils for BK
 */
class BKHelper {

    /**
     * Get operations from data frame. Operation can be in multiple entries.
     * @param item the item that hold the data frame
     * @param simple determine whether to print info of data entries.
     * @return the list of byte array hold one operation
     * @throws SerializationException return when unable to deserialize entry header
     */
    static List<ByteArraySegment> getSegments(LogReader.ReadItem item, boolean simple) throws SerializationException{

        DataFrame frame = new DataFrame(item);
        List<ByteArraySegment> segments = new LinkedList<>();

        while(true){
            DataFrame.DataEntry segment = frame.getEntry();
            if (segment == null){
                throw new SerializationException("Entry","Unable to serialize data frame entry");
            }
            if(!simple) segment.print();
            segments.add(segment.getData());

            if (segment.isLastRecordEntry()) break;
        }
        return segments;
    }

    /**
     * Get operation from byte array list.
     * @param segments the byte array segments hold one operation.
     * @return operation
     * @throws SerializationException return when cannot deserialize operation.
     */
    static Operation getOperation(List<ByteArraySegment> segments) throws SerializationException{
        Stream<InputStream> ss = segments
                .stream()
                .map(ByteArraySegment::getReader);
        return Operation.deserialize(
                new SequenceInputStream(
                        Iterators.asEnumeration(
                                ss.iterator()
                        )
                )
        );
    }
}
