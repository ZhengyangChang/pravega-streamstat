package service;

import lombok.Getter;

import java.io.PrintStream;
import java.util.Map;
import java.util.UUID;

/**
 * Formatted print utilities
 */
public class PrintHelper {

    private static final int MAX_PROCESS_LENGTH = 60;
    private static final PrintStream PRINT_STREAM = System.out;

    // region Print functions

    private static boolean active = true;
    public static void block(){
        active = false;
    }

    public static void unblock(){
        active = true;
    }

    /**
     * Print with color
     * @param color color
     * @param toPrint String to print
     *
     */
    public static void print(Color color, Object toPrint){
        if (active)
            PRINT_STREAM.format("%s%s%s",color.getAnsi(),toPrint, Color.RESET.getAnsi());
    }

    /**
     * plain print
     * @param toPrint String to print
     */
    public static void print(Object toPrint){
        if (active)
            PRINT_STREAM.print(toPrint);
    }

    public static void println(){
        if (active)
            PRINT_STREAM.println();
    }

    public static void println(Object s){
            print(s);
            println();
    }

    public static void println(Color color, Object s){
        print(color, s);
        println();
    }

    public static void format(String s,Object... objects){
        print(String.format(s,objects));
    }
    public static void format(Color color,String s,Object... objects) {
        print(color,String.format(s,objects));
    }
    // endregion

    // region Print by functionality
    static void printError(String s){
        println(Color.RED,s);
        println();
    }
    public static void printHead(String toPrint){
        print(Color.BLUE,String.format("%s: ",toPrint));
    }

    /**
     * print attribute string, override with different value type
     * @param attrName Attribute name
     * @param val Attribute value
     * @param isLast determine if it is the last attribute
     */
    public static void print(String attrName, Object val, boolean isLast){
        print(Color.GREEN, attrName);
        print(" = ");
        print(Color.YELLOW, val);
        if(!isLast) print(", ");
        else {
            println();
            println();
        }
    }

    public static void print(Map<UUID,Long> val){
        print("Attributes",val.toString(),true);
    }

    // endregion

    // region Process Indicator

    public static void processStart(String process){
        print(Color.PURPLE, String.format("%s ",process));
    }

    public static void processEnd(){
        print("\r");
        format("%" + MAX_PROCESS_LENGTH + "s"," ");
        print("\r");
    }

    // endregion

    public enum Color{
        RESET("\u001B[0m"),
        RED("\u001B[31m"),
        GREEN("\u001B[32m"),
        YELLOW("\u001B[33m"),
        BLUE("\u001B[34m"),
        PURPLE("\u001B[35m"),
        CYAN("\u001B[36m")
        ;
        @Getter
        private String ansi;
        Color(String ansi){
            this.ansi = ansi;
        }


    }
}
