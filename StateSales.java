package assignment63;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
 
public class StateSales implements WritableComparable<StateSales> {
 
    private String state;
    private int sales;
 
    public StateSales() {
    }

    public StateSales(String state, int sales) {
    	set(state, sales);
    }
    
    public String getState() {
        return state;
    }
 
    public int getSales() {
        return sales;
    }
 
    public void set(String state, int sales) {
        this.state = state;
        this.sales = sales;
    }
 
    @Override
    public void readFields(DataInput in) throws IOException {
    	state = in.readUTF();
    	sales = in.readInt();
    }
 
    @Override
    public void write(DataOutput out) throws IOException {
    	out.writeUTF(state);
    	out.writeInt(sales);
    }
 
    @Override
    public String toString() {
        return state + "\t" + sales;
    }
 
    @Override
    public int compareTo(StateSales ss) {
        return (-1) * (sales - ss.getSales());
    }

    @Override
    public boolean equals(Object o)
    {
        if(o instanceof StateSales)
        {
        	StateSales ss = (StateSales) o;
            return state.equalsIgnoreCase(ss.getState()) && (sales == ss.getSales());
        }
        return false;
    }
  
}