import java.io.*;


public class tester {
  public static void main(String args[]) {
     FileInputStream in = null;
     FileOutputStream out = null;

     try {
        in = new FileInputStream("../../data/sample.mix");
        out = new FileOutputStream("output.txt");
         
        parser p = new parser((InputStream)in,true);
        p.parseHTTP();
        p.parseXML();

        String mapperName = p.getMapperName();
        System.out.println(mapperName);

     } catch (IOException e) {
        System.out.println(e);
     }
  }
}
