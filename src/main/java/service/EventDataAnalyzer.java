package service;

import org.apache.commons.lang.SerializationUtils;
import record.DataRecord;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * static utility functions for analyze event data
 */
class EventDataAnalyzer {

    /**
     * Get the data record with data.
     * @param data the byte array of the data.
     * @return data record.
     * @throws SerializationException throw when cannot read data length.
     */
    private static DataRecord getDataRecord(byte[] data) throws SerializationException {
        StringBuilder sb = new StringBuilder();

        int bytesRead = 0;

        try {
            sb.append("[");
            DataInputStream s = new DataInputStream(new ByteArrayInputStream(data));
            while (s.available() > 0) {
                int left = s.available();

                if(left < Long.BYTES) break;

                bytesRead += Long.BYTES;

                long longLength = s.readLong();
                int length = (int) longLength;
                byte[] objectData = new byte[length];
                int read = s.read(objectData);

                if (read != length || length == 0) break;

                bytesRead += read;

                try {
                    Object o = SerializationUtils.deserialize(objectData);
                    sb.append(o.getClass().getName());
                    sb.append(":\"");
                    sb.append(o);
                    sb.append("\"");
                } catch (org.apache.commons.lang.SerializationException e) {
                    sb.append(String.format("(%d bytes event)", longLength));
                }

                if (s.available() > 0) sb.append(" ");

            }
            sb.append("]");
        } catch(IOException e){
            throw new SerializationException("Data", "Unable to read data length");
        }

        return new DataRecord(sb.toString(),0,bytesRead);
    }

    /**
     * Return data with only length.
     * @param data the data byte array to print
     * @return the data record (Short toPrint String)
     * @throws SerializationException throws when cannot serialize data
     */
    private static DataRecord getDataLengthRecord(byte[] data) throws SerializationException{
        int events = 0;
        long bytesRead = 0;
        try {
            DataInputStream s = new DataInputStream(new ByteArrayInputStream(data));
            while (s.available() > 0) {
                int left = s.available();

                if(left < Long.BYTES) break;

                bytesRead += Long.BYTES;
                long longLength = s.readLong();
                int length = (int) longLength;
                byte[] objectData = new byte[length];
                int read = s.read(objectData);

                if (read != length || length == 0) break;
                bytesRead += read;
                events ++;
            }

        } catch(IOException e){
            throw new SerializationException("Data", "Unable to read data");
        }

        return new DataRecord(String.format("Data Consist of %d bytes and %d events", bytesRead, events),events,bytesRead);
    }



    /**
     * Print data read (from both T1 and T2)
     * @param data byte array to print
     * @throws SerializationException throw when cannot deserialize data
     */
    static DataRecord getDataRecord(byte[] data, boolean printData) throws SerializationException{
        if (printData) return getDataRecord(data);
        else return getDataLengthRecord(data);
    }

}
