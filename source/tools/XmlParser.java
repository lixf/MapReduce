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
import java.util.Hashtable;
import java.util.ArrayList;

public class XmlParser {
  private BufferedReader reader;
  private String method;
  //params map type to value
  private ArrayList<Object> params;
  //don't care about version number just yet
  private boolean request;
    
  private String fault;
  private ArrayList<Object> result;


  public XmlParser(InputStream is) {
    reader = new BufferedReader(new InputStreamReader(is));
    method = "";
    params = new ArrayList<Object>();
  }

  //return int non-zero for error  
  public int parseRequest() throws IOException {
    String initial, prms[], cmd[], temp[];
    int ret, indexFront,indexBack, i;
    boolean seenMethodCall = false;
    boolean seenMethodName = false;
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
        if (line.substring(indexFront,indexBack).equals("methodCall")){
            if(seenMethodCall){
                //error malformed xml
                return 1;
            }
            else {
                //just use handler
                ret = parseMethodCall();
                seenMethodCall = true;
            }
        }
        else {
            return 1;
        }
    }
    return ret;
  }

  private int parseMethodCall() throws IOException{
    int ret, indexFront,indexBack, i;
    String line;
    ret = 0;

    //parse MethodName
    line = reader.readLine();
    indexFront = line.indexOf('>') + 1;
    indexBack  = line.indexOf('/') - 1;

    method = line.substring(indexFront,indexBack);

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
        else if (line.substring(indexFront,indexBack).equals("/methodCall")){
          return ret;
        }
        else {
            return 1;
        }
    }
    return ret;
  }


  //return int non-zero for error  
  public int parseResponse() throws IOException {
    String initial, prms[], cmd[], temp[];
    int ret, indexFront,indexBack, i;
    boolean seenMethodResponse = false;
    ret = 0;
    this.request = false;
    this.result = new ArrayList<Object> ();

    //start parsing, skip comment on first line
    String line = reader.readLine();

    while ((line = reader.readLine()) != null){
        //example <methodResponse>
        indexFront = line.indexOf('<') + 1;
        indexBack  = line.indexOf('>');

        //empty bracket?
        if (indexFront > indexBack) {
            throw new IOException();
        }
        
        //catch method call
        if (line.substring(indexFront,indexBack).equals("methodResponse")){
            if(seenMethodResponse){
                //error malformed xml
                return 1;
            }
            else {
                //just use handler
                ret = parseMethodResponse();
                seenMethodResponse = true;
            }
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
        else if (line.substring(indexFront,indexBack).equals("/methodCall")){
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
        
        //malformed xml
        if (indexFront > indexBack) {
            throw new IOException();
        }
        
        //put in hashtable
        if (this.request) {
            params.add(value);
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

}
