/*!Xiaofan Li
 * 15440 P3 Map-Reduce
 * Summer 2014
 *
 * Client side map reduce
 */

import java.net.ServerSocket;
import java.net.Socket;
import java.io.*;
import java.lang.RuntimeException;
import java.lang.Class;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.lang.NullPointerException;
import java.nio.charset.StandardCharsets;

public class MapReduceClient {

    private Socket cs;          //client
    private String serverIP;
    private int port;
    private boolean stopped;
    private String serial;
    private parser p;
    private String data;
    
    //I might need the params and the results stored in here
    private ArrayList<Object> params;
    private ArrayList<Object> results;

    //only has one constructor which takes IP and port
    //the constructor establishes connection
    public MapReduceClient(String serverIP, int port){
        this.port = port;
        this.serverIP = serverIP;
        
        //establish connection and handshake
        try {
            this.cs = new Socket(this.serverIP,this.port);
        } catch (IOException e){
            System.out.println("client socket error "+e);
        }
        System.out.println("got a socket to server at "+this.serverIP);
    }
    
    public void setDataPath(String data){
        this.data = data;
    }
    
    public Object send(String mapperName,Object mapper,String reducerName,Object reducer){
        //use a printer to generate the XML true for request
        printer p = new printer(true);

        //input from server save at this path 
        long millis = System.currentTimeMillis() % 1000;
        this.serial = String.valueOf(millis);
        String path = "../data/"+serial+"_request"+".mix";
        File log = new File(path);
        
        System.out.println("constructing new request");
        //send request to server on socket this.cs
        try {
            PrintWriter outSocket = new PrintWriter(this.cs.getOutputStream(), true);
            
            PrintWriter save = new PrintWriter(log);
            System.out.println("before printing xml");
            p.printClientRequest(mapperName,mapper,reducerName,reducer,this.data); 
            System.out.println("before printing http");
            String request = p.printHTTP();
            InputStream stream = new ByteArrayInputStream(request.getBytes(StandardCharsets.UTF_8));
            BufferedReader buffedIn = new BufferedReader (new InputStreamReader(stream));

            String temp;
            while((temp = buffedIn.readLine()) != null){
                save.println(temp);
                outSocket.println(temp);
            }

            //outSocket.write(request);
            //outSocket.flush();
            System.out.println("wrote request to socket");
            //outSocket.close();
            save.close();
            
            BufferedReader in = new BufferedReader(new InputStreamReader(this.cs.getInputStream()));
            //read response
            System.out.println("going to read response");
            while(!in.ready()){}
            //for debugging and logging, write the stream to a file
            String path2 = "../data/"+serial+"_response"+".mix";
            File log2 = new File(path2);
            PrintWriter save2 = new PrintWriter(log2);
            
            while(!(temp = in.readLine()).contains("/Response")){
                save2.println(temp);
            }
            save2.println(temp);
            save2.close();
            System.out.println("saved a response at "+path2);
            System.out.println("parsing response");

            InputStream toParser = new FileInputStream(path2);
            this.p = new parser(toParser,false); //this is response
            this.p.parseHTTP();
            this.p.parseXML();
            System.out.println("finished parsing response");

            this.results = this.p.getResult();
        } catch (IOException e){
            e.printStackTrace();
        } catch (NullPointerException e){
            e.printStackTrace();
        }

        
        Object ret = this.results.get(0); 
        //parse the XML reply and get a return object
        System.out.println("returning from execute "+ret.toString());
        return ret;
    }

}
