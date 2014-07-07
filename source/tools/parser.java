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
    private String method;
    private String fault;
    private ArrayList<Object> params;
    private ArrayList<Object> result;

    private String xmlContent;

    //am i parsing a request?
    private boolean request;

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
        ArrayList<String> dataNodeAdr;
        BufferedReader reader = new BufferedReader(new InputStreamReader(rawInput));
        //read line by line until see DataNodeIP :
        int i = 0;
        while ((line = reader.readLine())!=null) {
            index = line.indexOf("DataNodeIP");
            if (index >= 0) {
              index = line.indexOf(':');
              String dataIP = line.substring(index+1,line.length());
              dataNodeAdr[i] = dataIP;
              i = i+1;
            }  
        }
        return dataNodeAdr;
    }

    public String findServerIP() throws IOException{
        String line;
        int index;
        BufferedReader reader = new BufferedReader(new InputStreamReader(rawInput));
        //read line by line until see Port :
        while ((line = reader.readLine())!=null) {
            index = line.indexOf("serverIP");
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
                method = px.getMethod();
                params = px.getParams();
            }
            else{
                px.parseResponse();
                //empty if no fault otherwise return the fault as string
                this.fault = px.getFault();
                this.result = px.getResult();
            }
        }
    }
    
    public String getMethod(){
        return method;
    }
    
    public ArrayList<Object> getParams(){
        return params;
    }
   
    public ArrayList<Object> getResult(){
        return result;
    }
}
