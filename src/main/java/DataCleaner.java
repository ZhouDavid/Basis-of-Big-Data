import com.jcraft.jsch.ConfigRepository;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

/**
 * Created by Administrator on 2017/1/4.
 */
public class DataCleaner {
    public String InfoPath = "E:\\BusPredict\\raw_data\\apInfo";
    public static Map<String,BusInfoRecord> busInfoRecordMap = new HashMap<String, BusInfoRecord>();
    public void ReadFile()throws Exception{
        /*
            将busInfo文件中的车辆信息读入busInforRecords中
         */
        File file = new File(InfoPath);
        FileInputStream fs = new FileInputStream(file);
        InputStreamReader reader = new InputStreamReader(fs,"utf8");
        BufferedReader bufferedReader = new BufferedReader(reader);
        String line;
        while((line = bufferedReader.readLine())!=null){
            String attrs[] = line.split(",",-1);
            BusInfoRecord bir = new BusInfoRecord(attrs);
            busInfoRecordMap.put(bir.md5_mac,bir);
        }
//        for(int i = 0;i<busInfoRecords.size();i++){
//            System.err.println(busInfoRecords.get(i).md5_mac);
//        }
    }

    public static class RawRecordMapper extends Mapper<Object,Text,Text,Text>{
        public String parseTime(String time){
            String[] ttime = new String[6];
            ttime[0] = time.substring(0,4);
            ttime[1] = time.substring(4,6);
            ttime[2] = time.substring(6,8);
            ttime[3] = time.substring(8,10);
            ttime[4] = time.substring(10,12);
            ttime[5]= time.substring(12,14);
            String result = "";
            for(int i = 0;i<ttime.length;i++){
                result+=ttime[i]+",";
            }
            return result;
        }
        public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
            String line = new String(value.getBytes(),0,value.getLength(),"utf8");
            if(line.length()>10){
                String[] attrs = line.split(",",-1);
                String md5_mac = attrs[2];
                String rest =parseTime(attrs[0])+attrs[4]+","+attrs[5]+","+attrs[6]+","+attrs[7]+","+attrs[8];
                Text txt = new Text(rest);
                context.write(new Text(md5_mac),txt);
            }
        }
    }

    public static class RawRecordReducer extends Reducer<Text,Text,Text,Text>{
        public String busNameSearch(String mac){
            if(busInfoRecordMap.containsKey(mac)){
                return busInfoRecordMap.get(mac).busName;
            }
            else return null;
        }
        @Override
        public void reduce(Text key, Iterable<Text>values,Context context)throws IOException,InterruptedException{
            String busName = busNameSearch(key.toString());
            for(Text value:values){
                if(busName!=null){
                    String output = busName+","+ value.toString();
                    System.err.println("write");
                    context.write(key,new Text(output));
                }
            }
        }
    }
    public static void main(String args[])throws Exception{
        DataCleaner cleaner = new DataCleaner();
        cleaner.ReadFile();//info信息读入

        //Mapreduce 配置准备
        System.setProperty("hadoop.home.dir", "D:\\hadoop-3.0.0-alpha1");
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.ignoreseparator","true");
        conf.set("mapred.textoutputformat.separator",",");
        Job job = new Job(conf,"cleanData");
        job.setJarByClass(DataCleaner.class);
        job.setMapperClass(DataCleaner.RawRecordMapper.class);
//        job.setCombinerClass(BusPredict.Reduce.class);
        job.setReducerClass(DataCleaner.RawRecordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


