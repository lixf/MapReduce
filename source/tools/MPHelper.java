import java.io.Serializable;
import java.util.ArrayList;

//this class provides support to make MP runnable and migratable
public class MPHelper implements Serializable {
    private MigratableProcess mp;
    private String name;
    private Thread t;
    private String args;
    private boolean done;
    private final static long serialVersionUID = 1;

    public MPHelper(MigratableProcess mp, String name) {
        this.mp = mp;
        this.t = null;
        this.name = name;
        this.done = false;
    }


    public synchronized void start() {
        this.t = new Thread(mp);
        t.start();
        done = true;
    }

    
    public synchronized void stop() {
        t = null;
    }

    public synchronized ArrayList<String> getTypes() {
        while(!this.done){}
        return mp.getTypes();
    }

    public synchronized ArrayList<Object> getResult() {
        while(!this.done){}
        return mp.getResult();
    }

    public synchronized void setArgs(String args) {
        this.args = args;
    }


}
