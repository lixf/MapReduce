/*! @file nameNodeServer.java
 *  @author Xiaofan Li
 *  @date 15440 Summer 2014
 *  @brief This is an implementation of nameNodeServer
 *         start() is main loop, will block
 *         Only parses requests, does not know about config.txt
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


public class nameNodeServer implements Runnable {

    private ServerSocket ss;                        //server
    private ArrayList<Socket> dataNodeSoc;          //client
    private ArrayList<String> dataNodeAdr;
    private ArrayList<Integer> dataRecord;
    private int portIn;
    private int portOut;

    private boolean stopped;
    private String serial;
    private parser p;
    
    //I might need the params and the results stored in here
    private ArrayList<Object> params;
    private ArrayList<String> types;
    private ArrayList<Object> result;

    //map reduce 
    private Object mapper;
    private Object reducer;
    
    
    /* A lot of helper frunctions to simplify life */
    private InputStream talkTo(int index){
        //wait for input from dataNodeSoc[index]

    }


    private InputStream talkTo(int index, String msg){
        //write to dataNodeSoc[index] and then wait for reply


    }

    //wrapper for generating log files
    //it doesn't close the printwriter
    private String initLog(String location) {
        //initializes log file name
        long millis = System.currentTimeMillis() % 1000;
        this.serial = String.valueOf(millis);
        String path = "../data/"+serial+location+".log";
        return path;
    }
    
    
    /* *****************END OF HELPERS******************** */
    
    //constructor with port specified by config.txt
    public nameNodeServer(int portIn,int portOut,ArrayList<String> dataNodeAdr) {
        this.portIn = portIn;
        this.portOut = portOut;
        this.dataNodeAdr = dataNodeAdr;
        this.dataNodeSoc = new ArrayList<Socket>();

        System.out.println("data copied, ready to connect to data node");
        System.out.println("connecting to " + dataNodeAdr.length() + " nodes");
        try{
            for (int i=0;i<dataNodeAdr.length();i++) {
                this.dataNodeSoc[i] = new Socket(this.dataNodeAdr[i],this.portOut);
                System.out.println("connected to " + i + "th node at " + this.dataNodeAdr[i]);
            }
        } catch (IOException e){
            e.printStatckTrace();
        }
        System.out.println("connected to all dataNodes, starting to accept requests");
        for (int i=0;i<dataNodeAdr.length();i++) {
            //init record
            this.dataRecord[i] = 0;
        }

        try {
            this.ss = new ServerSocket(this.portIn);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.stopped = false;
        System.out.println("server up with port "+ port + "initializing data structure");
        params = new ArrayList<Object>();
        result = new ArrayList<Object>();
        System.out.println("server ready");
    }

    //supports threading
    public void run() {
        while (!this.stopped) {
            try {
                cs = this.ss.accept();
            } catch (IOException e) {
                if (this.stopped) {
                    System.out.println("Server Stopped");
                    return;
                }
                throw new RuntimeException ("cannot connect to client");
            }
            System.out.println("new client connected");

            //try (with resources) get input stream
            try (
                BufferedReader in = new BufferedReader(
                    new InputStreamReader (cs.getInputStream()));
            ) {
                String path = initLog("_name_req");
                //generate a log file
                PrintWriter save = new PrintWriter(new File(path));
                while (!in.ready()){}
                String temp;
                //for debugging and logging, write the stream to a file
                while(!(temp = in.readLine()).contains("</job>")){
                    save.println(temp);
                }
                save.println(temp);
                save.close(); 
                System.out.println("save a job request at "+path);
                
                //parse
                InputStream toParser = new FileInputStream(path);
                System.out.println("parsing request");
                this.p = new parser(toParser,true);
                this.p.parseHTTP();
                this.p.parseXML();
                System.out.println("finished parsing request");

                this.data = p.getData();
                //get the transported object and run it
                this.mapperName = p.getMapperName();
                this.reducerName = p.getReducerName();
                this.mapperObj = p.getMapperObj();
                this.reducerObj = p.getReducerObj();
                //a bunch of actual calls
                handleMapper();
                getMapperResult();
                handleReducer();
                getReducerResult();
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
    //send the mapper and reduce to data nodes and keep record
    private void handleMapper() {
        //form requests to data nodes 
        printer p = new Printer(this.param,true);
        //schedule nodes with current job loads
        //currently not doing anything because not parallel yet
        ArrayList<int> current;

        for (int i=0;i<dataNodeAdr.length();i++) {
            //incr record
            this.dataRecord[i] ++;
            current.add(i);
        }
        
        //send the requests to each node
        for (int i=0;i<current.length();i++) {
           p.printMapperRequest(this.mapperName,this.mapperObj,this.data,i);
           String request = p.printHTTP();

           //send request
           //convert the request to InputStream
           BufferedReader buffedIn = null;
           try {
               InputStream stream = new ByteArrayInputStream(request.getBytes(StandardCharsets.UTF_8));
               buffedIn = new BufferedReader (new InputStreamReader(stream));
           } catch (IOException e){
               System.out.println("handle back IO error");
           }
           String path = initLog("_mapper_"+i);;

           //get writer and wirte back a bunch 
           File log = new File(path);
           try {
               PrintWriter save = new PrintWriter(log);
               //get a writer
               PrintWriter sockout = new PrintWriter(this.dataNodeSoc[i].getOutputStream(),true);
               //for debugging and logging, write the stream to a file
               String temp;
               while((temp = buffedIn.readLine()) != null){
                   save.println(temp);
                   sockout.println(temp);
               }
               System.out.println("save an result at "+path);
               System.out.println("finished sending request to data node "+i); 
           } catch (IOException e){
               System.out.println("save result error "+e);
           }
        }


        //get results from mappers
        for (int i=0;i<current.length();i++) {
            //generate a log file
            long millis = System.currentTimeMillis() % 1000;
            this.serial = String.valueOf(millis);
            String path = "../data/"+serial+"_name_result.log";
            File log = new File(path);
            //try (with resources) get input stream
            try (
                BufferedReader in = new BufferedReader(
                    new InputStreamReader (dataNodeSoc[i].getInputStream()));
            ) {
                PrintWriter save = new PrintWriter(log);
                while (!in.ready()){}
                String temp;
                //for debugging and logging, write the stream to a file
                while(!(temp = in.readLine()).contains("</result>")){
                    save.println(temp);
                }
                save.println(temp);
                save.close(); 
                System.out.println("save a result from mapper at "+path);
                
                //parse
                InputStream toParser = new FileInputStream(path);
                System.out.println("getting result");
                this.p = new parser(toParser,true);
                this.p.parseHTTP();
                this.p.parseXML();
                System.out.println("finished parsing result");
                //put results in data structure for reducer
            }
        }
    }
      /////////////old////////////////////////////
      //we have parser and all its data structures
      //method should be object.method format
      //String method = p.getMethod();
      //System.out.println("serving: "+method);

      //int index = method.indexOf('.');

      //if (index <= 0) {
      //  System.out.println("cannot find obj info, aborting");
      //  return;
      //}

      //String objName = method.substring(0,index);
      //String methodName = method.substring(index+1,method.length());
      //
      ////get stub and call the stub
      //String stubName = objName + "ServerStub";
      //
      ////Class<?> procClass = null;
      ////Constructor<?> procCon = null;
      //Object obj = null;

      //try {
      //    obj = Class.forName(stubName).newInstance();
      //} catch (InstantiationException e) {
      //    e.printStackTrace();
      //} catch (IllegalArgumentException e) {
      //    e.printStackTrace();
      //} catch (ClassNotFoundException e) {
      //    e.printStackTrace();
      //} catch (IllegalAccessException e) {
      //    e.printStackTrace();
      //}
      //    
      //    //now pass in the arguments
      //try {
      //    Method meth0 = obj.getClass().getMethod("putArgs",ArrayList.class);
      //    meth0.invoke(obj,params);

      //    Method meth1 = obj.getClass().getMethod("execute",String.class);
      //    this.result = (ArrayList<Object>)meth1.invoke(obj,methodName);
      //    
      //    Method meth2 = obj.getClass().getMethod("getTypes");
      //    this.types = (ArrayList<String>)meth2.invoke(obj);
      //    
      //    //and call the method
      //    //this returns the xml result
      //    //this.result = H.execute(methodName);
      //    //this.types = H.getTypes();
      //} catch (Exception e){
      //    handleException(e);
      //}

    
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
