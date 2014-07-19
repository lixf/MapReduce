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
    private ArrayList<Socket> clientSoc;            //client 
    private int clientCnt;
    private ArrayList<Socket> dataNodeSoc;          //client to data node
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
    private ArrayList<String> final_types;
    private ArrayList<Object> final_result;

    //map reduce 
    private Object mapperObj;
    private Object reducerObj;
    private String data;
    private String mapperName;
    private String reducerName;
    private String reducerRequest;
    
    
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
        this.dataRecord = new ArrayList<Integer>();

        System.out.println("data copied, ready to connect to data node");
        System.out.println("connecting to " + dataNodeAdr.size() + " nodes");
        if (dataNodeAdr.size() == 0){
            System.out.println("no data node info, check config.txt");
            System.exit(1);
        }
        
        try{
            for (int i=0;i<dataNodeAdr.size();i++) {
                this.dataNodeSoc.add(i,new Socket(this.dataNodeAdr.get(i),this.portOut));
                System.out.println("connected to " + i + "th node at " + this.dataNodeAdr.get(i));
            }
        } catch (IOException e){
            e.printStackTrace();
        }
        for (int i=0;i<dataNodeAdr.size();i++) {
            //init record
            this.dataRecord.add(i,0);
        }
        System.out.println("connected to all dataNodes and initialized record");

        try {
            this.ss = new ServerSocket(this.portIn);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        this.stopped = false;
        System.out.println("server up with port "+ this.portIn + " initializing data structure");
        this.params = new ArrayList<Object>();
        this.result = new ArrayList<Object>();
        this.types = new ArrayList<String>();
        this.final_result = new ArrayList<Object>();
        this.final_types = new ArrayList<String>();
        this.clientSoc = new ArrayList<Socket>();
        this.dataRecord = new ArrayList<Integer>();
        System.out.println("server ready");
    }

    //supports threading
    public void run() {
        while (!this.stopped) {
            try {
                Socket cs = this.ss.accept();
                this.clientSoc.add(this.clientCnt,cs);
            } catch (IOException e) {
                if (this.stopped) {
                    System.out.println("Server Stopped");
                    return;
                }
                throw new RuntimeException ("cannot connect to client");
            }
            System.out.println("new client connected: "+this.clientCnt+"th client");
            handle(this.clientCnt);
            this.clientCnt++;
        }
    }

    private void handle(int clientID){
        //try (with resources) get input stream
        try (
            BufferedReader in = new BufferedReader(
                new InputStreamReader (this.clientSoc.get(clientID).getInputStream()));
        ) {
            String path = initLog("_name_req");
            //generate a log file
            PrintWriter save = new PrintWriter(new File(path));
            while (!in.ready()){}
            String temp;
            System.out.println("start to accept request from the client");

            //for debugging and logging, write the stream to a file
            while(!(temp = in.readLine()).contains("</job>")){
                //System.out.println(temp);
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
            handleReducer();
            handleSendBack(clientID);
        } catch (IOException e){
            System.out.println("reading/Writing problem "+e);
            return;
        }

    }


    public void stop() {
      //close server port and free data structures TODO
      if (this.clientSoc.size()!=0){
        try {
           for (int i=0;i<this.clientSoc.size();i++){
                this.clientSoc.get(i).close();
           }
           this.clientSoc.clear();
        } catch (IOException e){
            System.out.println("socket problem");
        }
      }
    }

    public static int countLines(String filename) {
        int count = 0;
        int readChars = 0;
        boolean empty = true;
        
        try {
            InputStream is = new BufferedInputStream(new FileInputStream(filename));
            byte[] c = new byte[1024];
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            
            is.close();
        } catch (IOException e){
            e.printStackTrace();
        }
        return (count == 0 && !empty) ? 1 : count;
    }

    //generate new file name for data
    private String split_name(int index){
        int period = this.data.indexOf('.');
        String first = this.data.substring(0,period);
        String second = this.data.substring(period,this.data.length());
        return (first+"_"+index+second);
    }

    //function to split the data and create data for each data node 
    //and put the file in ./data/ for transmission
    private void split_data(BufferedReader input,int num_mapper,int each_lines){
        String temp;
        try{
            for (int index=0;index<num_mapper;index++){
                int count = 0;
                String new_path = split_name(index);
                File new_data = new File("../data/"+new_path);
                PrintWriter file = new PrintWriter(new_data);
                while(count<each_lines && ((temp = input.readLine())!=null)){
                    file.println(temp);
                    count++;
                }
                System.out.println("spliting data: "+ new_path);
                file.close();
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }


    //parses and handles the request
    //send the mapper and reduce to data nodes and keep record
    private void handleMapper() {
        System.out.println("going to handle mapper");
        //form requests to data nodes 
        printer p = new printer(true);
        //schedule nodes with current job loads
        ArrayList<Integer> current = new ArrayList<Integer>();
        
        //scheduling policy TODO
        for (int i=0;i<dataNodeAdr.size();i++) {
            //just use all nodes
            current.add(i);
        }
        int num_mapper = current.size();
        
        //split the data
        String datapath = "../data/"+this.data;
        int tot_lines = countLines(datapath);
        System.out.println("spliting data tot line: "+ tot_lines);
        int each_lines = 0;
        if (tot_lines%num_mapper == 0){
            each_lines = tot_lines/num_mapper;
        } else {
            each_lines = (tot_lines/num_mapper) + 1;
        }
        System.out.println("spliting data each line: "+ each_lines);
        try {
            InputStream dataStream = new FileInputStream("../data/"+this.data);
            BufferedReader in = new BufferedReader(new InputStreamReader(dataStream));

            split_data(in,num_mapper,each_lines);
            
            dataStream.close();
        } catch (IOException e){
            e.printStackTrace();
        }
        
        //send the requests to each node
        System.out.println("going to send mapper req to each mapper");
        for (int i=0;i<current.size();i++) {

           //send request
           //convert the request to InputStream
           BufferedReader buffedIn = null;
           try {
               p.printMapperRequest(this.mapperName,this.mapperObj,this.data,i);
               String request = p.printHTTP();
               InputStream stream = new ByteArrayInputStream(request.getBytes(StandardCharsets.UTF_8));
               buffedIn = new BufferedReader (new InputStreamReader(stream));
           } catch (IOException e){
               System.out.println("handle back IO error");
               e.printStackTrace();
           }
           String path = initLog("_mapper_"+i);;

           //get writer and wirte back a bunch 
           File log = new File(path);
           try {
               PrintWriter save = new PrintWriter(log);
               //get a writer
               PrintWriter sockout = new PrintWriter(this.dataNodeSoc.get(i).getOutputStream(),true);
               //for debugging and logging, write the stream to a file
               String temp;
               while((temp = buffedIn.readLine()) != null){
                   save.println(temp);
                   sockout.println(temp);
               }
               save.close();
               System.out.println("save an result at "+path);
               System.out.println("finished sending request to data node "+i); 
           } catch (IOException e){
               System.out.println("save result error "+e);
           }
        }


        //get results from mappers
        for (int i=0;i<current.size();i++) {
            //generate a log file
            long millis = System.currentTimeMillis() % 1000;
            this.serial = String.valueOf(millis);
            String path = "../data/"+serial+"_name_result.log";
            File log = new File(path);
            //try (with resources) get input stream
            try {
                BufferedReader in = new BufferedReader(
                    new InputStreamReader (dataNodeSoc.get(i).getInputStream()));
                PrintWriter save = new PrintWriter(log);
                while (!in.ready()){}
                String temp;
                //for debugging and logging, write the stream to a file
                while(!(temp = in.readLine()).contains("</Response>")){
                    save.println(temp);
                }
                save.println(temp);
                save.close(); 
                System.out.println("save a result from mapper at "+path);
                
                //parse
                InputStream toParser = new FileInputStream(path);
                System.out.println("getting result");
                this.p = new parser(toParser,false);
                this.p.parseHTTP();
                this.p.parseXML();
                this.types.addAll(this.p.getTypes());
                this.result.addAll(this.p.getResult());
                System.out.println("finished parsing result");
                //put results in data structure for reducer
            } catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    private void handleReducer(){
        System.out.println("going to handle reducer");
        //form requests to data node
        printer p = new printer(true);
        //schedule nodes with current job loads
        ArrayList<Integer> current = new ArrayList<Integer>();
        
        //send the requests to each node
        System.out.println("going to send reducer req to 0th data node");
           
        //send request
        //convert the request to InputStream
        BufferedReader buffedIn = null;
        try {
            p.printReducerRequest(this.reducerName,this.reducerObj,
                                                 this.result,this.types);
            String request = p.printHTTP();
            InputStream stream = new ByteArrayInputStream(request.getBytes(StandardCharsets.UTF_8));
            buffedIn = new BufferedReader (new InputStreamReader(stream));
        } catch (IOException e){
            System.out.println("handle back IO error");
            e.printStackTrace();
        }
        String path = initLog("_reducer");;

        //get writer and wirte back a bunch 
        File log = new File(path);
        try {
            PrintWriter save = new PrintWriter(log);
            //get a writer
            PrintWriter sockout = new PrintWriter(this.dataNodeSoc.get(0).getOutputStream(),true);
            //for debugging and logging, write the stream to a file
            String temp;
            while((temp = buffedIn.readLine()) != null){
                save.println(temp);
                sockout.println(temp);
            }
            save.close();
            System.out.println("save an request at "+path);
            System.out.println("finished sending request to data node"); 
        } catch (IOException e){
            System.out.println("save request error "+e);
        }

        //generate a log file
        long millis = System.currentTimeMillis() % 1000;
        this.serial = String.valueOf(millis);
        path = initLog("_name_result");
        File log2 = new File(path);
        //try (with resources) get input stream
        try {
            BufferedReader in = new BufferedReader(
                new InputStreamReader (dataNodeSoc.get(0).getInputStream()));
            PrintWriter save = new PrintWriter(log2);
            while (!in.ready()){}
            String temp;
            //for debugging and logging, write the stream to a file
            while(!(temp = in.readLine()).contains("</Response>")){
                save.println(temp);
            }
            save.println(temp);
            save.close(); 
            System.out.println("save a result from reducer at "+path);
            
            //parse
            InputStream toParser = new FileInputStream(path);
            System.out.println("getting result");
            this.p = new parser(toParser,false);
            this.p.parseHTTP();
            this.p.parseXML();
            this.final_types = this.p.getTypes();
            this.final_result = this.p.getResult();
            System.out.println("finished parsing result");
            //put results in data structure for reducer
        } catch (IOException e){
            e.printStackTrace();
        }
        return;
    }
    
    //send back results from a object
    private void handleSendBack(int clientID) {
        //convert the result to InputStream
        printer p = new printer(false);
        BufferedReader buffedIn = null;
        try {
            p.printXML(this.final_result,this.final_types);
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
            PrintWriter sockout = new PrintWriter(this.clientSoc.get(clientID).getOutputStream(),true);
            //for debugging and logging, write the stream to a file
            String temp;
            while((temp = buffedIn.readLine()) != null){
                save.println(temp);
                sockout.println(temp);
            }
            save.close();
            sockout.close();
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
