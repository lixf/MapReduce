

import java.lang.Runnable;
import java.util.ArrayList;
import java.io.*;

public class FindReducer implements Reducer{

    private boolean exist;
    private String data;
    private ArrayList<Object> param;
    private ArrayList<String> param_types;
    private ArrayList<Object> result;
    private ArrayList<String> result_types;
    private final static long serialVersionUID = 1;

    public FindReducer(){
        this.param = new ArrayList<Object>();
        this.param_types = new ArrayList<String>();
        this.result = new ArrayList<Object>();
        this.result_types = new ArrayList<String>();
        this.exist = true;
    }

    public void setArgs(ArrayList<Object> param, ArrayList<String> types){
        this.param = param;
        this.param_types = types;
    }
    
    public void run(){
        for (int i=0;i<this.param.size();i++){
            if (!this.param_types.get(i).equals("Boolean")){
                //fault
                System.exit(2);
            }
        }
        
        for (int i=0;i<this.param.size();i++){
            this.exist = (this.exist || (boolean)this.param.get(i));
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
