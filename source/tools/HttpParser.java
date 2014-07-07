import java.io.*;
import java.util.*;
import java.text.*;
import java.net.URLDecoder;

public class HttpParser {
  private BufferedReader reader;
  private String method, url;
  private Hashtable <String,String> headers;
  private int[] ver;
  private String content;
  //for client side
  private int respCode;
  private String serverName;


  public HttpParser(InputStream is) {
    reader = new BufferedReader(new InputStreamReader(is));
    method = "";
    url = "";
    headers = new Hashtable<String,String>();
    //version
    ver = new int[2];
  }


  public int parseRequest() throws IOException {

    String initial, prms[], cmd[], temp[];
    int ret, idx, i;

    ret = 200; // default is OK now
    initial = reader.readLine();
    if (initial == null || initial.length() == 0) return 0;
    if (Character.isWhitespace(initial.charAt(0))) {
      // starting whitespace, return bad request
      return 400;
    }
    
    //catch bad req
    cmd = initial.split("\\s");
    if (cmd.length != 3) {
      return 400;
    }
    
    //only works with RPC2
    if (!cmd[1].equals("/RPC2")) ret = 400;
    
    if (cmd[2].indexOf("HTTP/") != 0) ret = 400;
    else {
        //TODO
        int v0 = cmd[2].indexOf('/')+1;
        int v1 = cmd[2].indexOf('/')+3;
        ver[0] = Integer.parseInt(cmd[2].substring(v0,v0+1));
        ver[1] = Integer.parseInt(cmd[2].substring(v1,v1+1));
    }

    //parse req
    if (cmd[0].equals("POST")) {
      parseHeaders();
      putContent();
      if (headers == null) ret = 400;
    }
    else {
      ret = 400;
    }

    if (ver[0] != 1 || (ver[1] != 0 && ver[1] != 1) || getHeader("Host") == null) {
      ret = 400;
    }

    return ret;
  }

  public int parseResponse() throws IOException{
    String initial, prms[], cmd[], temp[];
    int ret, idx, i;

    ret = 200; // default is OK now
    initial = reader.readLine();
    if (initial == null || initial.length() == 0) return 0;
    if (Character.isWhitespace(initial.charAt(0))) {
      // starting whitespace, return bad response
      return 400;
    }
    
    //catch bad resp
    cmd = initial.split("\\s");
    if (cmd.length != 3) {
      return 400;
    }
    
    //get respCode
    if (!cmd[1].equals("200")){
      ret = 0;
      this.respCode = 0;
    }
    else this.respCode = 200;
    
    if (cmd[0].indexOf("HTTP/") != 0) ret = 0;
    else {
        //magic
        int v0 = cmd[0].indexOf('/')+1;
        int v1 = cmd[0].indexOf('/')+3;
        ver[0] = Integer.parseInt(cmd[0].substring(v0,v0+1));
        ver[1] = Integer.parseInt(cmd[0].substring(v1,v1+1));
    }

    //parse req
    if (cmd[2].equals("OK")) {
      parseHeaders();
      putContent();
      if (headers == null) ret = 0;
    }
    else {
      ret = 0;
    }

    if (ver[0] != 1 || (ver[1] != 0 && ver[1] != 1)) {
      ret = 0;
    }

    return ret;

  }

  private void parseHeaders() throws IOException {
    String line;
    int idx;

    line = reader.readLine();
    while (!line.equals("")) {
      idx = line.indexOf(':');
      if (idx < 0) {
        headers = null;
        break;
      }
      else {
        headers.put(line.substring(0, idx).toLowerCase(), line.substring(idx+1).trim());
      }
      line = reader.readLine();
    }
  }

  //this put the rest of reader to content ready to be extracted
  private void putContent() throws IOException {
    String line;

    line = reader.readLine();

    line = reader.readLine();
    content = line;
    while (line != null){
        content = content + line + "\n";
        line = reader.readLine();
    }
  }


  //a bunch of helpers to communicate outside
  public String getMethod() {
    return method;
  }

  public String getHeader(String key) {
    if (headers != null)
      return headers.get(key.toLowerCase());
    else return null;
  }

  public Hashtable<String,String> getHeaders() {
    return headers;
  }

  public String getContent(){
    return content;
  }
    
  public String getVersion() {
    return ver[0] + "." + ver[1];
  }

  public int compareVersion(int major, int minor) {
    if (major < ver[0]) return -1;
    else if (major > ver[0]) return 1;
    else if (minor < ver[1]) return -1;
    else if (minor > ver[1]) return 1;
    else return 0;
  }

  public int getRespCode() {
    return this.respCode;
  }

}
