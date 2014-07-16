/*!\brief This is a parser interface for parsing 
 *        XML wrapped in HTTP POST requests
 * \author Xiaofan Li
 */

import java.io.InputStream;
import java.io.IOException;
import java.util.Hashtable;
import java.util.ArrayList;
import java.nio.charset.StandardCharsets;
import java.io.ByteArrayInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class parser {
    //raw input received on socket
    private InputStream rawInput; 
    
    //for http
    private String version;
    private String userAgent;
    private String host;
    private int length;
    private int respCode;
    private String serverName;
    
    //for xml
    private String fault;
    private String mapperName;
    private String reducerName;
    private Object mapperObj;
    private Object reducerObj;
    private String data;
    private ArrayList<Object> params;
    private ArrayList<Object> result;

    private String xmlContent;

    //am i parsing a request?
    private boolean request;

    //am i parsing for a mapper or reducer?
    private boolean mapper;

/*!\brief Public constructor 
 */
    public parser (InputStream rawInput,boolean request) {
        this.rawInput = rawInput;
        this.request = request;
    }
/*!\brief Utilities to parse the config file
 */
    public int findPortIn() throws IOException{
        String line;
        int index;
        BufferedReader reader = new BufferedReader(new InputStreamReader(rawInput));
        //read line by line until see Port :
        while ((line = reader.readLine())!=null) {
            index = line.indexOf("NameNodePort");
            if (index >= 0) {
              index = line.indexOf(':');
              String port = line.substring(index+1,line.length());
              return Integer.parseInt(port);
            }  
        }
        return 0;
    }
    
    public int findPortOut() throws IOException{
        String line;
        int index;
        BufferedReader reader = new BufferedReader(new InputStreamReader(rawInput));
        //read line by line until see Port :
        while ((line = reader.readLine())!=null) {
            index = line.indexOf("DataNodePort");
            if (index >= 0) {
              index = line.indexOf(':');
              String port = line.substring(index+1,line.length());
              return Integer.parseInt(port);
            }  
        }
        return 0;
    }

    public ArrayList<String> findDataNodeAdr() throws IOException{
        String line;
        int index;
        ArrayList<String> dataNodeAdr = new ArrayList<String>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(rawInput));
        //read line by line until see DataNodeIP :
        int i = 0;
        while ((line = reader.readLine())!=null) {
            index = line.indexOf("DataNodeIP");
            if (index >= 0) {
              index = line.indexOf(':');
              String dataIP = line.substring(index+1,line.length());
              dataNodeAdr.add(i,dataIP);
              i = i+1;
            }  
        }
        return dataNodeAdr;
    }

    public String findNameNodeAdr() throws IOException{
        String line;
        int index;
        BufferedReader reader = new BufferedReader(new InputStreamReader(rawInput));
        //read line by line until see Port :
        while ((line = reader.readLine())!=null) {
            index = line.indexOf("NameNodeIP");
            if (index >= 0) {
              index = line.indexOf(':');
              String serverIP = line.substring(index+1,line.length());
              return serverIP;
            }  
        }
        return "";
    }

/*!\brief This function parses the HTTP POST request
 *        and populates the private variables
 * \require parser class with rawInput initialized
 * \returns List of Strings including:
 *          version, user-agent, host, length, and the content
 * \exception If length != length(content)
 *            Or content-type != text/xml
 */
    public void parseHTTP () throws IOException{
        HttpParser hp = new HttpParser(rawInput);
        if (this.request){
            //this call should populate everything in HttpParser
            hp.parseRequest();
            
            //then we populate our own class with data
            //and prepare for parsing xml
            //first get the request
            version = hp.getVersion();
            userAgent = hp.getHeader("User-Agent");
            host = hp.getHeader("host");
            length = Integer.parseInt(hp.getHeader("content-length"));
            xmlContent = hp.getContent();
        }
        else{
            hp.parseResponse();
            //then populate the responses
            respCode = hp.getRespCode();
            serverName = hp.getHeader("server");
            //length = Integer.parseInt(hp.getHeader("content-length"));
            xmlContent = hp.getContent();
        }
        System.out.println("exiting parseHTTP");
    }
    
    //define helpers to return desired data
    public String getVersion(){
        return this.version;
    }

    public String getUserAgent(){
        return this.userAgent;
    }

    public String getHost(){
        return this.host;
    }

    public int getLength(){
        return this.length;
    }

    public int getRespCode(){
        return this.respCode;
    }

    public String getServerName(){
        return this.serverName;
    }

    public String getXmlContent(){
        return this.xmlContent;
    }


    //Start the XML parser
    public void parseXML() throws IOException {
        if(xmlContent == null){
            throw new IOException();
        }
        else{
            InputStream stream = new ByteArrayInputStream(xmlContent.getBytes(StandardCharsets.UTF_8));
            XmlParser px = new XmlParser(stream);
            if (this.request) {
                px.parseRequest();

                //make all the parallel data call
                this.mapper = px.IsMapper();
                this.mapperName = px.getMapperName();
                this.reducerName = px.getReducerName();
                this.mapperObj = px.getMapperObj();
                this.reducerObj = px.getReducerObj();
                this.data = px.getData();
            }
            else{
                px.parseResponse();
                //empty if no fault otherwise return the fault as string
                this.fault = px.getFault();
                this.result = px.getResult();
            }
        }
        System.out.println("exiting paseXML"+this.mapperName+this.data);
    }
    
    public String getMapperName(){
        return this.mapperName;
    }

    public String getReducerName(){
        return this.reducerName;
    }
    
    public Object getMapperObj(){
        return this.mapperObj;
    }
    
    public Object getReducerObj(){
        return this.reducerObj;
    }
    
    public String getData(){
        return this.data;
    }
   
    public ArrayList<Object> getResult(){
        return result;
    }
     
    public boolean IsMapper(){
        return this.mapper;
    }
    
    //for mappers
    public int getMyIdx(){
        int dot = this.data.indexOf('.');
        int under = this.data.indexOf('_');
        String index = this.data.substring(under+1,dot);
        return Integer.valueOf(index);
    }
    
    public String getObjName(){
        if (this.mapper){
            return getMapperName();
        }
        else{
            return getReducerName();
        }
    }
    
    public Object getObject(){
        if (this.mapper){
            return getMapperObj();
        }
        else{
            return getReducerObj();
        }
    }

}
