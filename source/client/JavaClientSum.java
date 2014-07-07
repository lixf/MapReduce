/*!Xiaofan Li
 *
 *
 */

import java.util.ArrayList;
import java.io.*;

public class JavaClientSum {
 public static void main (String [] args) {
  try {
     //read config file and parse part and IP
     String configPath = "../config.txt";
     String serverIP = getServerIP(configPath);
     int port = getPort(configPath);

     xmlRpcClient server = new xmlRpcClient(serverIP,port);
     
     //use for a specific functionality
     ArrayList<Object> params = new ArrayList<Object>();
     ArrayList<String> types = new ArrayList<String>();
     params.add(new Integer(17));
     params.add(new Integer(13));

     types.add(0,"int");
     types.add(1,"int");

     System.out.println("Going to execute RPC call");
     //execute will cause the client to call the client stub
     Object result =  server.execute("Sum.sum", params,types);
     System.out.println("RPC call returned");
     
     //This is returned by the stub
     int sum = Integer.parseInt(result.toString());
     System.out.println("The sum is: "+ sum);

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
        serverIP = p.findServerIP();
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
        port = p.findPort();
    } catch (IOException e){
        System.out.println("port error");
    }
    return port;
  }

}
