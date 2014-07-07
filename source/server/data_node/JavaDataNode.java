/*!Xiaofan Li
 *\brief XMLRPC server side runnable Blocks until control-C
 *\require Pre-exchanged IP addr and parsing of config file
 */

import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;

public class JavaServer {

  public static void main (String [] args) {
     try {
       int port = getPort("../config.txt");
       System.out.println("Attempting to start XML-RPC Server...");
       xmlRpcServer server = new dataNodeServer(port);
       server.run();//this will block
     } catch (Exception exception) {
       System.err.println("JavaDataNode: " + exception);
     }
  }
  

  private static int getPort(String path){
    int port;
    try {
        File config = new File(path);
        InputStream toParser = new FileInputStream(path);
        parser p = new parser(toParser,false);
        port = p.findPortOut();
    } catch (IOException e){
        System.out.println(e);
        return 0;
    }
        return port;
  }
}
