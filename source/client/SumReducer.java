

import java.lang.Runnable;
import java.util.ArrayList;
import java.io.*;

public class SumReducer implements MigratableProcess{

    private int sum;
    private String data;
    private final static long serialVersionUID = 1;

    public SumReducer(){
        sum = 0;
    }

    public void setArgs(String data){
        this.data = data;
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
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(this.data)));
            String line;
            while ((line = reader.readLine()) !=null){
                int curr = count_line(line);
                this.sum =+ curr;
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
        res.add(Integer.valueOf(this.sum));
        return res;
    }
}
