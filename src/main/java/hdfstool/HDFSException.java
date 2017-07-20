package hdfstool;

/**
 * Exception for hdfs file read
 */
public class HDFSException extends Exception {
    public boolean isMissingFile;
    public HDFSException(String src,String msg, boolean isMissingFile){
        super(String.format("%s: %s",src,msg));
        this.isMissingFile = isMissingFile;
    }
}
