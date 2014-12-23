
import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Aditia Rakhmat S
 */
public class MinMaxCountMapper extends TableMapper<Text, MinMaxCountTupple>{
    
    private Text outUserId = new Text();
    private MinMaxCountTupple outTupple = new MinMaxCountTupple();
    
    private final static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:MM:ss");

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        String userId = new String(value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("user")));
        int minimum = Bytes.toInt(value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("minimum")));
        int maximum = Bytes.toInt(value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("maximum")));
        int count = Bytes.toInt(value.getValue(Bytes.toBytes("cf"), Bytes.toBytes("count")));
        
        System.out.println("M-> "+userId+" "+minimum+" "+maximum+" "+count);
        
        outTupple.setMin(minimum);
        outTupple.setMax(maximum);
        outTupple.setCount(count);
        outUserId.set(userId);
        context.write(outUserId, outTupple);
        
    }
    
}
