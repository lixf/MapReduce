/*! Xiaofan Li
 *  15440 summer 2014
 *\brief Parser for simple XML format request forms
 *\require <MethodCall>,<MethodName>,<Params>,<Param>
 *         <Value>,<types>
 *
 *\ensure Returns Method name and a hashtable of all 
 *        type & value pairs 
 */


import java.io.*;
import java.util.*;
import java.text.*;
import java.net.URLDecoder;
import java.lang.ClassNotFoundException;

public class XmlParser {
  private BufferedReader reader;
  private String method;
  //params map type to value
  private ArrayList<Object> params;
  //don't care about version number just yet
  private boolean request;
    
  private String fault;
  private ArrayList<Object> result;
  private ArrayList<String> types;

  //for data-node
  private String mapperName;
  private String reducerName;
  private Object mapperObj;
  private Object reducerObj;
  private String data;
  private boolean isMapper;
  private boolean dataNode;

  public XmlParser(InputStream is) {
    reader = new BufferedReader(new InputStreamReader(is));
    method = "";
    params = new ArrayList<Object>();
    this.result = new ArrayList<Object> ();
    this.types = new ArrayList<String> ();
  }

  //return int non-zero for error  
  public int parseRequest() throws IOException {
    String initial, prms[], cmd[], temp[];
    int ret, indexFront,indexBack, i;
    ret = 0;
    this.request = true;

    //start parsing, skip comment on first line
    String line = reader.readLine();
    line = reader.readLine();

    while ((line = reader.readLine()) != null){
        //example <methodCall>
        indexFront = line.indexOf('<') + 1;
        indexBack  = line.indexOf('>');

        //empty bracket?
        if (indexFront > indexBack) {
            throw new IOException();
        }
        
        //catch method call
        if (line.substring(indexFront,indexBack).equals("job")){
            //just use handler
            this.dataNode = false;
            ret = parseJob();
        }
        else if (line.substring(indexFront,indexBack).equals("DataNodeJob")){
            //just use handler
            this.dataNode = true;
            ret = parseDataNodeJob();
        }
        else {
            return 1;
        }
    }
    return ret;
  }

  private int parseJob() throws IOException{
    System.out.println("parsing job");
    int ret, indexFront,indexBack, i;
    String line;
    ret = 0;

    //parse mapper or reducer
    line = reader.readLine();
    indexFront = line.indexOf('<') + 1;
    indexBack  = line.indexOf('>');

    this.method = line.substring(indexFront,indexBack);

    if (this.method.contains("mapper")){
        parseMapper();
        //waste the last line
        line = reader.readLine();
        
        if((line = reader.readLine()).contains("reducer")){
            parseReducer();
        }
        parseData();
    }
    else {
        parseReducer();
        //waste the last line
        line = reader.readLine();
        if((line = reader.readLine()).contains("mapper")){
            parseMapper();
        }
        else if (line.contains("params")){
        
        }
    }
    return ret;
  }
  
  private int parseDataNodeJob() throws IOException{
    System.out.println("parsing data node job");
    int ret, indexFront,indexBack, i;
    String line;
    ret = 0;

    //parse mapper or reducer
    line = reader.readLine();
    if (line.contains("mapper")){
        this.isMapper = true;
        parseMapper();
        parseData();
    }
    else {
        this.isMapper = false;
        parseReducer();
        line = reader.readLine();
        if ((line=reader.readLine()).contains("params")){
            ret = parseParams();
        }
    }
    return ret;
  }
  
  private void parseMapper() throws IOException{
    System.out.println("parsing mapper");
    int ret, indexFront,indexBack, i;
    String line;

    //parse mapper or reducer
    line = reader.readLine();
    indexFront = line.indexOf('>') + 1;
    indexBack  = line.indexOf('/') - 1;

    this.mapperName = line.substring(indexFront,indexBack);
    
    //read the base64 data
    line = reader.readLine();
    indexFront = line.indexOf('>') + 1;
    indexBack  = line.indexOf('/') - 1;
    String bin = line.substring(indexFront,indexBack);

    this.mapperObj = string2bin(bin);
  }
  
  private void parseReducer() throws IOException{
    System.out.println("parsing reducer");
    int ret, indexFront,indexBack, i;
    String line;

    //parse mapper or reducer
    line = reader.readLine();
    indexFront = line.indexOf('>') + 1;
    indexBack  = line.indexOf('/') - 1;

    this.reducerName = line.substring(indexFront,indexBack);
    
    //read the base64 data
    line = reader.readLine();
    indexFront = line.indexOf('>') + 1;
    indexBack  = line.indexOf('/') - 1;
    String bin = line.substring(indexFront,indexBack);

    this.reducerObj = string2bin(bin);
  }

  private Object string2bin(String st) throws IOException{
    byte [] data = Base64Coder.decode(st);
    ObjectInputStream ois = new ObjectInputStream( 
                            new ByteArrayInputStream(data));
    Object o = null;
    try{
        o = ois.readObject();
    } catch (ClassNotFoundException e){
        e.printStackTrace();
    }

    ois.close();
    return o;
  }

  private void parseData() throws IOException{
    if(this.dataNode){
        
        int ret, indexFront,indexBack, i;
        String line;

        //skip two lines
        line = reader.readLine();
        line = reader.readLine();
        line = reader.readLine();

        indexFront = line.indexOf('>') + 1;
        indexBack  = line.indexOf('/') - 1;

        this.data = line.substring(indexFront,indexBack);

        //write the rest to file
        File save = new File("../data/"+this.data);
        try{
            PrintWriter f = new PrintWriter(save);
            while (!(line=reader.readLine()).contains("</data-content>")){
                f.println(line);
            }
            f.close();
         } catch (IOException e){
            e.printStackTrace();
         }
    }
    else{
        int ret, indexFront,indexBack, i;
        String line;

        //skip a line
        line = reader.readLine();
        line = reader.readLine();

        indexFront = line.indexOf('>') + 1;
        indexBack  = line.indexOf('/') - 1;

        this.data = line.substring(indexFront,indexBack);
    }
  }


  //return int non-zero for error  
  public int parseResponse() throws IOException {
    String initial, prms[], cmd[], temp[];
    int ret, indexFront,indexBack, i;
    ret = 0;
    this.request = false;

    //start parsing, skip comment on first line
    String line = reader.readLine();

    while ((line = reader.readLine()) != null){
        indexFront = line.indexOf('<') + 1;
        indexBack  = line.indexOf('>');

        //empty bracket?
        if (indexFront > indexBack) {
            throw new IOException();
        }
        
        //catch method call
        if (line.substring(indexFront,indexBack).equals("Response")){
            //just use handler
            ret = parseMethodResponse();
        }
        else {
            return 1;
        }
    }
    return ret;
  }


  private int parseMethodResponse() throws IOException{
    int ret, indexFront,indexBack, i;
    String line;
    ret = 0;

    //parse the next line might be fault or params
    while ((line = reader.readLine()) != null){
        //example <params>
        indexFront = line.indexOf('<') + 1;
        indexBack  = line.indexOf('>');

        //empty bracket?
        if (indexFront > indexBack) {
            throw new IOException();
        }
        
        //catch method call
        if (line.substring(indexFront,indexBack).equals("params")){
            //just use handler
            ret = parseParams();
        }
        else if (line.substring(indexFront,indexBack).equals("fault")){
            ret = parseFault(); //TODO
        }
        else if (line.substring(indexFront,indexBack).equals("/Response")){
          return ret;
        }
        else {
            return 1;
        }
    }
    return ret;
  }

  private int parseFault() throws IOException{
    return 0;
  }


  private int parseParams() throws IOException{
    int ret, indexFront,indexBack, i;
    String line;
    ret = 0;
    
    while ((line = reader.readLine()) != null){
        //example <params>
        indexFront = line.indexOf('<') + 1;
        indexBack  = line.indexOf('>');

        //empty bracket?
        if (indexFront > indexBack) {
            throw new IOException();
        }
        
        //catch method call
        if (line.substring(indexFront,indexBack).equals("param")){
            //just use handler
            ret = parseOneParam();
        }
        else if (line.substring(indexFront,indexBack).equals("/params")){
          return ret;
        }
        else {
            return 1;
        }
    }
    return ret;
  }


  private int parseOneParam() throws IOException{
    int ret, indexFront,indexBack, i;
    String line;
    
    line = reader.readLine();
        //example <value><type>val</type><value>
        indexFront = line.indexOf("<value>") + 7; //skip "<value>"
        indexBack  = line.indexOf("</value>");

        //malformed xml
        if (indexFront > indexBack) {
            throw new IOException();
        }
        
        //get substring with type and value
        String sub = line.substring(indexFront,indexBack);
        
        //get the type
        indexFront = sub.indexOf("<") + 1;
        indexBack  = sub.indexOf(">");
        String type = sub.substring(indexFront,indexBack);
        int typelen = type.length();
        if (type.contains("i4")){
           this.types.add("Integer");
        }else if (type.contains("string")){
           this.types.add("String");
        }else {
           this.types.add("Boolean");
        }

        
        //malformed xml
        if (indexFront > indexBack) {
            throw new IOException();
        }
        
        //get value
        String delimFront = "<"+type+">";
        String delimBack  = "</"+type+">";
        indexFront = sub.indexOf(delimFront) + 1;
        indexBack  = sub.indexOf(delimBack);
        String value = sub.substring(indexFront+typelen+1,indexBack);
        Object o = null;

        if (type.contains("i4")){
           o = (Object) Integer.valueOf(value);
        }else if (type.contains("string")){
           o = (Object) value;
        }else {
           o = (Object) Boolean.valueOf(value);
        }
        
        //malformed xml
        if (indexFront > indexBack) {
            throw new IOException();
        }
        
        //put in hashtable
        if (this.request) {
            params.add(o);
        }
        else {
            this.result.add(0,value);
        }

        //check and return
        line = reader.readLine();
        indexFront = line.indexOf('<') + 1;
        indexBack  = line.indexOf('>');

        if (line.substring(indexFront,indexBack).equals("/param")){
          return 0;
        }
        else {
            return 1;
        }
  }



  //a bunch of helpers to communicate outside
  public String getMethod() {
    return method;
  }

  public ArrayList<Object> getParams() {
    return params;
  }

  public String getFault(){
    return this.fault;
  }

  public ArrayList<Object> getResult(){
    return this.result;
  }

  public ArrayList<String> getTypes(){
    return this.types;
  }

  public boolean IsMapper(){
    return this.isMapper;
  }
  
  public String getMapperName() {
    return this.mapperName;
  }

  public String getReducerName() {
    return this.reducerName;
  }

  public String getData() {
    return this.data;
  }

  public Object getMapperObj(){
    return this.mapperObj;
  }
  
  public Object getReducerObj(){
    return this.reducerObj;
  }
}
