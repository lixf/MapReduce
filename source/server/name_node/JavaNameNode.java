/*!@file JavaNameNode.java
 *
 * @author Xiaofan Li
 * @date   July 9th 2014
 *
 * @brief name node server side runnable Blocks until control-C
 *        require Pre-exchanged IP addr and parsing of config file
 */

import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;


public class JavaNameNode {

  public static void main (String [] args) {
     try {
       //default port
       int portIn = 8080;
       int portOut = 8081;
       
       System.out.println("Parsing config file now");
       
       String path = "../config.txt";
       ArrayList<String> dataNodeAdr = null;
       try {
            InputStream toParser = new FileInputStream(path);
            parser p = new parser(toParser,false);
            portIn = p.findPortIn();

            toParser = new FileInputStream(path);
            p = new parser(toParser,false);
            portOut = p.findPortOut();

            toParser = new FileInputStream(path);
            p = new parser(toParser,false);
            dataNodeAdr = p.findDataNodeAdr();
       } catch (IOException e) {
            e.printStackTrace();
       }

       System.out.println("Attempting to start MapReduce name Server...");
       nameNodeServer server = new nameNodeServer(portIn,portOut,dataNodeAdr);
       server.run();//this will block
     } catch (Exception exception) {
       System.err.println("JavaServer: " + exception);
     }
  }
}
