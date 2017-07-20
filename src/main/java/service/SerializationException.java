package service;

/**
 * Exception
 */
public class SerializationException extends Exception {
    public SerializationException(String src,String msg){
        super(String.format("%s: %s",src,msg));
    }
}
