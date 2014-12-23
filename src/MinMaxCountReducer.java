
import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Aditia Rakhmat S
 */
public class MinMaxCountReducer extends TableReducer<Text, MinMaxCountTupple, ImmutableBytesWritable> {

    //output value writable
    private MinMaxCountTupple result = new MinMaxCountTupple();
    
    @Override
    protected void reduce(Text key, Iterable<MinMaxCountTupple> values, Context context) throws IOException, InterruptedException {
        //Initialize result
        result.setMax(-100);
        result.setMin(100);
        int sum = 0;
        
        for (MinMaxCountTupple val : values) {
        
            if (val.getMin() < result.getMin()) {
                result.setMin(val.getMin());
            }
            
            if(val.getMax() > result.getMax()){
                result.setMax(val.getMax());
            }
            
            sum  += val.getCount();
            
        }
        
        result.setCount(sum);
        
        Put put = new Put(toBytes(key.toString()));
        put.add(Bytes.toBytes("cf"),toBytes("min"),toBytes(result.getMin()));
        put.add(Bytes.toBytes("cf"),toBytes("max"),toBytes(result.getMax()));
        put.add(Bytes.toBytes("cf"),toBytes("count"),toBytes(result.getCount()));
        
        System.out.println("R-> "+key.toString()+" "+result.getMin()+" "+result.getMax()+" "+result.getCount());
        
        context.write(null, put);
    }
    
}
