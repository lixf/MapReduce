import java.util.ArrayList;
import java.lang.Runnable;
import java.io.Serializable;



public interface Reducer extends java.lang.Runnable, java.io.Serializable{
        ArrayList<String> getTypes();
        ArrayList<Object> getResult();
        void setArgs(ArrayList<Object> params, ArrayList<String> types);
        void run();
}

