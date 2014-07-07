/*!Xiaofan Li
 * 15440 P2 XML-RPC
 * Summer 2014
 *
 * Client side rpc
 */

import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.io.File;
import java.lang.RuntimeException;
import java.lang.Class;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.lang.NullPointerException;

public class xmlRpcClient {

    private Socket cs;          //client
    private String serverIP;
    private int port;
    private boolean stopped;
    private String serial;
    private parser p;
    
    //I might need the params and the results stored in here
    private ArrayList<Object> params;
    private ArrayList<Object> results;

    //only has one constructor which takes IP and port
    //the constructor establishes connection
    public xmlRpcClient(String serverIP, int port){
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
    
    //execute does the following:
    //      1. get object+method name being called, 
    //      2. calls the appropriate stub to convert the request to a XML stream
    //      3. sends the stream to server
    //      4. wait to get a response back
    //      5. return the response as object

    public Object execute(String name, ArrayList<Object> params, ArrayList<String> types){
        //parse the object and method to call
        int index = name.indexOf('.');
        String obj = name.substring(0,index);
        String method = name.substring(index+1,name.length());

        //use a printer to generate the XML true for request
        printer p = new printer(params,true);

        //input from server save at this path 
        long millis = System.currentTimeMillis() % 1000;
        this.serial = String.valueOf(millis);
        String path = "../data/"+serial+"_response"+".mix";
        File log = new File(path);
        
        System.out.println("constructed new request");
        //send request to server on socket this.cs
        try ( 
            PrintWriter outSocket = new PrintWriter(this.cs.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(this.cs.getInputStream()));
        ) {
            PrintWriter save = new PrintWriter(log);
            System.out.println("before printing xml");
            p.printXML(name,types); 
            System.out.println("before printing http");
            String request = p.printHTTP();
            outSocket.write(request);
            outSocket.flush();
            System.out.println("wrote request to socket");
            
            //read response
            String temp;
            System.out.println("going to read response");
            while(!in.ready()){}
            //for debugging and logging, write the stream to a file
            while((temp = in.readLine()) != null){
                save.println(temp);
            }
            save.close();
            System.out.println("saved a response at "+path);
            System.out.println("parsing response");

            InputStream toParser = new FileInputStream(path);
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
