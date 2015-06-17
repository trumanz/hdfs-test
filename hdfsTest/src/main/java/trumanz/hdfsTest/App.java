package trumanz.hdfsTest;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException
    {
        AmbariWrapper ambari = new AmbariWrapper("http://172.17.0.1:8080", "admin", "admin");
        String  hdfs = ambari.getDefaultFS();
        System.out.println(hdfs);
    }
}
