
import java.io.IOException;
import java.math.BigDecimal;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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
public class Main {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     * @throws java.lang.ClassNotFoundException
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        
        Configuration conf = HBaseConfiguration.create();
        
        Job job = Job.getInstance(conf, "MinMaxCount");
        job.setJarByClass(Main.class);
        
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        
        TableMapReduceUtil.initTableMapperJob(
                Bytes.toBytes("min_max_count"),
                scan, 
                MinMaxCountMapper.class,
                Text.class, 
                 MinMaxCountTupple.class, 
                 job);
        
        TableMapReduceUtil.initTableReducerJob(
                "min_max_count_result", 
                MinMaxCountReducer.class, 
                job);
        
        //job.setNumReduceTasks(1);
        
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
        
        
    }
    
}
