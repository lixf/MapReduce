/*!@file dataNodeServer.java
 * @author Xiaofan Li
 * @date 15440 Summer 2014
 * @brief This is an implementation of dataNodeServer
 *        start() is main loop, will block
 *        Only parses requests, does not know about config.txt
 */
import java.net.ServerSocket;
import java.net.Socket;
import java.io.*;
import java.lang.RuntimeException;
import java.lang.Class;
import java.lang.reflect.Method;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.Hashtable;
import java.util.ArrayList;

/*!@brief this is possibly multi-threaded runnable
 * meaning that one machine could run multiple dataNodes
 * but right now I'm not doing that
 */
public class dataNodeServer implements Runnable {

    private ServerSocket ss;    //server
    private Socket cs;          //client
    private int port;
    private boolean stopped;
    private String serial;
    private parser p;
    
    //I might need the params and the results stored in here
    private ArrayList<Object> params;
    private ArrayList<String> types;
    private ArrayList<Object> result;

    //constructor with port specified by main
    public dataNodeServer(int port) {
        this.port = port;
        try {
            this.ss = new ServerSocket(this.port);
        } catch (IOException e) {
            throw new RuntimeException("cannot open port "+port);
        }
        this.stopped = false;
        System.out.println("data node up with port "+ port);
        params = new ArrayList<Object>();
        result = null;
        
        //wait for name node connection
        try {
            cs = this.ss.accept();
        } catch (IOException e) {
            throw new RuntimeException ("cannot connect to name node");
        }
        System.out.println("data-node: name node connected");
    }

    //supports threading
    public void run() {
        while (!this.stopped) {
            //generate a log file
            long millis = System.currentTimeMillis() % 1000;
            this.serial = String.valueOf(millis);
            String path = "../data/"+serial+"_data-node"+".mix";
            File log = new File(path);
            //try (with resources) get input stream
            try (
                BufferedReader in = new BufferedReader(
                    new InputStreamReader (cs.getInputStream()));
            ) {
                PrintWriter save = new PrintWriter(log);
                while (!in.ready()){}
                String temp;
                //for debugging and logging, write the stream to a file
                while(!(temp = in.readLine()).contains("</job>")){
                    save.println(temp);
                }
                save.println(temp);
                save.close(); 
                System.out.println("data-node: save an request at "+path);
                
                //parse
                InputStream toParser = new FileInputStream(path);
                System.out.println("data-node: parsing request");
                this.p = new parser(toParser,true);
                this.p.parseHTTP();
                this.p.parseXML();
                System.out.println("data-node: finished parsing request");
                //get data from parser
                this.mapper = p.IsMapper();
                this.idx = p.getMyIndex();
                this.objName = p.getObjName();
                this.obj = p.getObject();
                this.data = p.getMyData();

                handleRequest();
                handleSendBack();
                //close client connection TODO
                //cs.close();
            } catch (IOException e){
                System.out.println("reading/Writing problem "+e);
                return;
            }

        }

    }

    public void stop() {
      //close server port and free data structures TODO
      if (cs!=null){
        try { 
           cs.close();
        } catch (IOException e){
            System.out.println("socket problem");
        }
      }
    }

    //parses and handles the request
    //uses the stub to handle the conversion of arguments
    private void handleRequest() {
      //we have parser and all its data structures
      //method should be object.method format
      System.out.println("data-node: serving "+this.objName);
        
      //hard part
      Class<?> procClass = null;
      Constructor<?> procCon = null;
      ObjHelper H = null;
      
      //for dynamically determining the class
      //find class with string
      try {
          procClass = Class.forName(obj);
      } catch (ClassNotFoundException e) {
          System.out.println(command + " " + e);
          System.exit(1);
      }

      try {
          procCon = procClass.getConstructor(String[].class);
      } catch (SecurityException e) {
          e.printStackTrace();
      } catch (NoSuchMethodException e) {
          e.printStackTrace();
      }

      try {
          Object[] initArgs = new Object[1];
          initArgs[0] = this.data;
          H = new ObjHelper((MigratableProcess) procCon.newInstance(initArgs));
      } catch (InstantiationException e) {
          e.printStackTrace();
      } catch (IllegalArgumentException e) {
          e.printStackTrace();
      } catch (InvocationTargetException e) {
          e.printStackTrace();
      } catch (IllegalAccessException e) {
          e.printStackTrace();
      }
      
      H.start();
      this.result = H.getResult();
}
    
    //send back results from a object
    private void handleSendBack() {
        //convert the result to InputStream
        printer p = new printer(this.result,false);
        BufferedReader buffedIn = null;
        try {
            p.printXML(this.types);
            String resultXML = p.printHTTP();
            InputStream stream = new ByteArrayInputStream(resultXML.getBytes(StandardCharsets.UTF_8));
            buffedIn = new BufferedReader (new InputStreamReader(stream));
        } catch (IOException e){
            System.out.println("handle back IO error");
        }
        String path = "../data/result_"+this.serial+".mix";

        //send back ../data/result_<serial>.mix
        //get writer and wirte back a bunch 
        //try (with resources) get input stream
        File log = new File(path);
        try (
            PrintWriter save = new PrintWriter(log);
            //get a writer
            PrintWriter sockout = new PrintWriter(this.cs.getOutputStream(),true);
        ) {
            //for debugging and logging, write the stream to a file
            String temp;
            while((temp = buffedIn.readLine()) != null){
                save.println(temp);
                sockout.println(temp);
            }
            System.out.println("save an result at "+path);

            System.out.println("finished sending back result");
            System.out.println("request done\n\n");
        } catch (IOException e){
            System.out.println("save result error "+e);
        }
    }
    private void handleException(Exception e){
        System.out.println("handle called in xmlServer " + e);
    }
}
