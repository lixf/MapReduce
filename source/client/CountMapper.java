/**!@file CountMapper.java
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

public class CountMapper implements MigratableProcess{
    
    private String data;
    private int count;
    private final static long serialVersionUID = 1;

    public CountMapper(){
        count = 0;
    }

    public void setArgs(String data){
        this.data = data;
    }

    private int count_white(String input){
        int white = input.length() - input.replaceAll(" ", "").length();
        return (white +1);
    }

    public void run(){ 
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(this.data)));
            String line;
            while ((line = reader.readLine()) !=null){
                int curr = count_white(line);
                this.count =+ curr;
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public ArrayList<String> getTypes(){
        ArrayList<String> type = new ArrayList<String>();
        type.add("Integer");
        return type;
    }

    public ArrayList<Object> getResult(){
        ArrayList<Object> res = new ArrayList<Object>();
        res.add(Integer.valueOf(this.count));
        return res;
    }
}

