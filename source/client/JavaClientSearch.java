/*!Xiaofan Li
 *
 *
 */

import java.util.ArrayList;
import java.io.*;

public class JavaClientSearch {
 public static void main (String [] args) {
  try {
     //read config file and parse part and IP
     String configPath = "../config.txt";
     String serverIP = getServerIP(configPath);
     int port = getPort(configPath);
    
     //get mapper and reducer
     SearchMapper map = new SearchMapper("Glen");
     FindReducer reduce = new FindReducer();

     MapReduceClient server = new MapReduceClient(serverIP,port);
     
     //use for a specific functionality
     server.setDataPath("book.txt");

     System.out.println("Going to send request");
     //execute will cause the client to call the client stub
     Object result =  server.send("SearchMapper",map,"FindReducer",reduce);
     System.out.println("Name node returned");
     
     //This is returned by the stub
     String res = result.toString();
     System.out.println("The result is: "+ res);

   } catch (Exception exception) {
     System.err.println("JavaClient: " + exception);
   }
  }
  
  private static String getServerIP(String path){
    InputStream toParser = null;
    String serverIP = "";
    try {
        File config = new File(path);
        toParser = new FileInputStream(path);
        parser p = new parser(toParser,true);
        serverIP = p.findNameNodeAdr();
    } catch (IOException e){
        System.out.println("server ip error");    
    }
    return serverIP;
  }

  private static int getPort(String path){
    InputStream toParser = null;
    int port = 0;
    try {
        File config = new File(path);
        toParser = new FileInputStream(path);
        parser p = new parser(toParser,true);
        port = p.findPortIn();
    } catch (IOException e){
        System.out.println("port error");
    }
    return port;
  }

}
