
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.Writable;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Aditia Rakhmat S
 */
public class MinMaxCountTupple implements Writable{

    private long min = 0;
    private long max = 0;
    private long count = 0;
    
    public long getMin() {
        return min;
    }

    public void setMin(long min) {
        this.min = min;
    }

    public long getMax() {
        return max;
    }

    public void setMax(long max) {
        this.max = max;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
        d.writeLong(min);
        d.writeLong(max);
        d.writeLong(count);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        min = di.readLong();
        max = di.readLong();
        count = di.readLong();
    }

}
