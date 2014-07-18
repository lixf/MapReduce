/*!\brief This is a printer interface for parsing 
 *        XML wrapped in HTTP POST requests
 * \author Xiaofan Li
 */

import java.io.*;
import java.util.*;

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
    public printer (boolean request) {
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
    private String bin2str(Object binary) throws IOException{
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(binary);
        oos.close();
        return new String(Base64Coder.encode(baos.toByteArray()));
    }

    private String divData(String data, int index){
        int period = data.indexOf('.');
        String first = data.substring(0,period);
        String second = data.substring(period,data.length());
        return (first+"_"+index+second);
    }   
    
    public void printClientRequest(String mapperName, Object mapperObj, String reducerName,Object reducerObj, String data) throws IOException {
        System.out.println("entered printClientRequest");
        String content;
        //hard code these
        String xmlHeader = "<?xml version='1.0'?>\n<job>\n";
        String mapper = "<mapper>\n" + "<mapper-name>" + mapperName + "</mapperName>\n";
        String reducer = "<reducer>\n" + "<reducer-name>" + reducerName + "</reducerName>\n";
        
        try{
            mapper = mapper + "<mapperObj>"+bin2str(mapperObj)+"</mapperObj>\n</mapper>\n";
            reducer = reducer + "<reducerObj>"+bin2str(reducerObj)+"</reducerObj>\n</reducer>\n";
        } catch (IOException e){
            e.printStackTrace();
        }
        
        String yourData = "<data>"+data+"</data>\n";
      
        //append footer
        content = xmlHeader+mapper+reducer+yourData+"</job>";
        this.xmlContent = content;
        this.xmlLength = content.length();
        System.out.println("exited printClientRequest");
    }

    public void printMapperRequest(String mapperName, Object mapperObj, String data, int index) throws IOException {
        System.out.println("entered printMapperRequest "+ index+"th node");
        String content;
        //hard code these
        String xmlHeader = "<?xml version='1.0'?>\n<DataNodeJob>\n";
        String yourType = "<mapper>\n";
        String name = "<mapperName>"+mapperName+"</mapperName>\n";
        String obj = "<mapperObj>"+bin2str(mapperObj)+"</mapperObj>\n";
        String endType = "</mapper>\n";
        String yourData = "<data>"+divData(data,index)+"</data>\n";
      
        //append footer
        content = xmlHeader+yourType+name+obj+endType+yourData+"</DataNodeJob>";
        this.xmlContent = content;
        this.xmlLength = content.length();
        System.out.println("exited printMapperRequest");
    }
    
    public void printReducerRequest(String reducerName, Object reducerObj, ArrayList<Object> result,ArrayList<String> types) throws IOException {
        System.out.println("entered printReducerRequest");
        //hard code these
        String xmlHeader = "<?xml version='1.0'?>\n<DataNodeJob>\n";
        String yourType = "<reducer>\n";
        String name = "<reducerName>"+reducerName+"</reducerName>\n";
        String obj = "<reducerObj>"+bin2str(reducerObj)+"</reducerObj>\n";
        String endType = "</reducer>\n";
        
        //input data
        int numArgs = types.size();
        String content = xmlHeader+yourType+name+obj+endType+"<params>\n";

        for (int i=0;i<numArgs;i++) {
            String paramXML = printOneParam(types.get(i),result.get(i));
            //append the next parameter
            content = content + paramXML;
        }
      
        //append footer
        content = content+"</params>\n"+"</DataNodeJob>";
        this.xmlContent = content;
        this.xmlLength = content.length();
        System.out.println("exited printReducerRequest");
    }
    
    public void printXML(ArrayList<Object> params,ArrayList<String> types) throws IOException{
        
        System.out.println("entered printXML respond");
        String xmlHeader = "<?xml version='1.0'?>\n<Response>\n<params>\n";
        String xmlFooter = "</params>\n</Response>\n";
        int numArgs = types.size();
        String content = xmlHeader;

        for (int i=0;i<numArgs;i++) {
            String paramXML = printOneParam(types.get(i),params.get(i));
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
        if (type.equals("Integer")){
            stuff = (param).toString();
            return "<param>\n<value><i4>"+stuff+"</i4></value>\n</param>\n";
        } else if (type.equals("String")){
            stuff = (String)param;
            return "<param>\n<value><string>"+stuff+"</string></value>\n</param>\n";
        } else if (type.equals("Boolean")){
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
