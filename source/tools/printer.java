/*!\brief This is a printer interface for parsing 
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

public class printer {
    //raw inputs from parent prog
    private ArrayList<Object> rawInput; 
    
    //for http
    private String version;
    private String userAgent;
    private String host;
    private int xmlLength;
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
    public printer (ArrayList<Object> params,boolean request) {
        this.params = params;
        this.request = request;
    }

/*!\brief This function prints the HTTP POST request
 * \require printer class with rawInput initialized
 * \returns List of Strings including:
 *          version, user-agent, host, length, and the content
 * \exception If length != length(content)
 *            Or content-type != text/xml
 */
    public String printHTTP () throws IOException{
        String content = "";
        if (this.request){
            String first = "POST /RPC2 HTTP/1.0\nUser-Agent: kali\nHost:\nContent-Type: text/xml\nContent-length:";
            content = first + this.xmlLength + "\n\n\n\n" + this.xmlContent;
        }
        else {
            String first = "HTTP/1.1 200 OK\nConnection: close\nContent-Length:";
            String third = "Content-Type: text/xml\n\n\n";
            content = first + this.xmlLength + "\n" + third + this.xmlContent;
        }
        
        return content;
        
    }

    //helpers
    private String bin2str(Object binary){
        return;
    }

    private String divData(String data, int index){
        int period = data.indexOf('.');
        String first = data.substring(0,period);
        String second = data.substring(period+1,data.length());
        return (first+"_"+index+second);
    }   

    //Start the XML printer:
    //Takes in a types and match the one got from params
    public void printMapperRequest(String mapperName, Object mapperObj, String data, int index) throws IOException {
        System.out.println("entered printMapperRequest "+ index+"th node");
        String content;
        //hard code these
        String xmlHeader = "<?xml version='1.0'?>\n<job>\n";
        String yourInd = "<index>"+index+"</index>";
        String yourType = "<type>mapper</type>\n";
        String name = "<objName>"+mapperName+"</objName>\n";
        String obj = "<obj>"+bin2str(mapperObj)+"</obj>\n";
        String yourData = "<data>"+divData(data,index)+"</data>\n";
      
        //append footer
        content = xmlHeader+yourIdx+yourType+name+obj+yourData+"</job>";
        this.xmlContent = content;
        this.xmlLength = content.length();
        System.out.println("exited printMapperRequest");
    }
    
    public void printXML(ArrayList<String> types) throws IOException{
        
        System.out.println("entered printXML respond");
        String xmlHeader = "<?xml version='1.0'?>\n<methodResponse>\n<params>\n";
        String xmlFooter = "</params>\n</methodResponse>\n";
        int numArgs = types.size();
        String content = xmlHeader;

        for (int i=0;i<numArgs;i++) {
            String paramXML = printOneParam(types.get(i),this.params.get(i));
            //append the next parameter
            content = content + paramXML;
        }
        //append footer
        content = content + xmlFooter;
        this.xmlContent = content;
        this.xmlLength = content.length();
        System.out.println("exited printXML respond");
    }
   

    private String printOneParam (String type, Object param){
        System.out.println("entered print one param");
        String stuff = "";
        if (type.equals("int")){
            stuff = ((Integer)param).toString();
            return "<param>\n<value><i4>"+stuff+"</i4></value>\n</param>\n";
        } else if (type.equals("string")){
            stuff = (String)param;
            return "<param>\n<value><string>"+stuff+"</string></value>\n</param>\n";
        } else if (type.equals("boolean")){
            stuff = ((Boolean)param).toString();
            return "<param>\n<value><boolean>"+stuff+"</boolean></value>\n</param>\n";
        } else {
            return stuff;
        }
    }


    
    public ArrayList<Object> getParams(){
        return params;
    }
   
}
