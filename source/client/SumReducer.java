

import java.lang.Runnable;
import java.util.ArrayList;
import java.io.*;

public class SumReducer implements Reducer{

    private int sum;
    private String data;
    private ArrayList<Object> param;
    private ArrayList<String> param_types;
    private ArrayList<Object> result;
    private ArrayList<String> result_types;
    private final static long serialVersionUID = 1;

    public SumReducer(){
        this.param = new ArrayList<Object>();
        this.param_types = new ArrayList<String>();
        this.result = new ArrayList<Object>();
        this.result_types = new ArrayList<String>();
        this.sum = 0;
    }

    public void setArgs(ArrayList<Object> param, ArrayList<String> types){
        this.param = param;
        this.param_types = types;
    }
    
    private int count_line(String input){
        int local = 0;
        String[] parts = input.split(" ");
        for (int i=0;i<parts.length;i++){
            local =+ Integer.valueOf(parts[i]);
        }
        return local;
    }

    public void run(){
        for (int i=0;i<this.param.size();i++){
            if (!this.param_types.get(i).equals("Integer")){
                //fault
                System.exit(2);
            }
        }
        
        for (int i=0;i<this.param.size();i++){
            this.sum += (int)this.param.get(i);
        }

    }
    public ArrayList<String> getTypes(){
        ArrayList<String> type = new ArrayList<String>();
        type.add("Integer");
        return type;
    }

    public ArrayList<Object> getResult(){
        ArrayList<Object> res = new ArrayList<Object>();
        res.add(Integer.valueOf(this.sum));
        return res;
    }
}
