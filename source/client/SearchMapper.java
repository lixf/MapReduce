/**!@file SearchMapper.java
 *  @author Xiaofan Li
 *  @brief 
 */

import java.util.*;
import java.lang.Runnable;
import java.io.Serializable;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;

public class SearchMapper implements Mapper{
    
    private String data;
    private String key;
    private boolean exist;
    private final static long serialVersionUID = 1;

    public SearchMapper(String key){
        exist = false;
        this.key = key;
    }

    public void setArgs(String data){
        this.data = data;
    }

    public void run(){ 
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(this.data)));
            String line;
            while ((line = reader.readLine()) !=null){
              if (line.contains(this.key)){
                this.exist = true;
                return;
              }
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public ArrayList<String> getTypes(){
        ArrayList<String> type = new ArrayList<String>();
        type.add("Boolean");
        return type;
    }

    public ArrayList<Object> getResult(){
        ArrayList<Object> res = new ArrayList<Object>();
        res.add((Boolean)this.exist);
        return res;
    }
}

