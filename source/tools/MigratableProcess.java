import java.util.ArrayList;
import java.lang.Runnable;
import java.io.Serializable;



public interface MigratableProcess extends java.lang.Runnable, java.io.Serializable{
        ArrayList<String> getTypes();
        ArrayList<Object> getResult();
        void setArgs(String args);
        void run();
}

