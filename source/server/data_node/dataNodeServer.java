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

    //map/reduce
    private int myIdx;
    private String data;
    private String objName;
    private Object obj;
    private boolean mapper;

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
            System.out.println("running");
            //generate a log file
            long millis = System.currentTimeMillis() % 1000;
            this.serial = String.valueOf(millis);
            String path = "../data/"+serial+"_data-node"+".log";
            File log = new File(path);
            //try (with resources) get input stream
            try {
                BufferedReader in = new BufferedReader(
                    new InputStreamReader (cs.getInputStream()));
                PrintWriter save = new PrintWriter(log);
                while (!in.ready()){}
                String temp;
                //for debugging and logging, write the stream to a file
                while(!(temp = in.readLine()).contains("</DataNodeJob>")){
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
                //get data from parser
                this.mapper = this.p.IsMapper();
                System.out.println("is mapper?"+this.mapper);
                this.objName = this.p.getObjName();
                System.out.println(this.objName);
                this.obj = this.p.getObject();
                if (this.mapper){
                    this.data = this.p.getData();
                    this.myIdx = this.p.getMyIdx();
                } else {
                    this.result = this.p.getParams();
                    this.types = this.p.getTypes();
                }
                System.out.println("data-node: finished parsing request");

                handleRequest();
                handleSendBack();
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
      
      //just run the object passed in
      if (this.mapper){
        Mapper H = (Mapper) this.obj;
        try {
          H.setArgs("../data/"+this.data);
          H.run();  
        } catch (Exception e){
          e.printStackTrace();
        }
        
        this.result = H.getResult();
        this.types = H.getTypes();
      } 
      else {
        Reducer H = (Reducer) this.obj;
        try {
          H.setArgs(this.result,this.types);
          H.run();  
        } catch (Exception e){
          e.printStackTrace();
        }
        
        this.result = H.getResult();
        this.types = H.getTypes();
      }

    }


    //send back results from a object
    private void handleSendBack() {
        //convert the result to InputStream
        printer p = new printer(false);
        BufferedReader buffedIn = null;
        try {
            p.printXML(this.result,this.types);
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
        try {
            PrintWriter save = new PrintWriter(log);
            //get a writer
            PrintWriter sockout = new PrintWriter(this.cs.getOutputStream(),true);
            //for debugging and logging, write the stream to a file
            String temp;
            while((temp = buffedIn.readLine()) != null){
                save.println(temp);
                sockout.println(temp);
            }
            System.out.println("save an result at "+path);
            save.close();

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
