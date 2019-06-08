package bigdata.coursera.flight;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CountByAirport {
    public static class Map extends Mapper<LongWritable, Text,Text,IntWritable> {
        public static final Log log = LogFactory.getLog(Map.class);
        private Text word = new Text();
        private final static IntWritable one = new IntWritable(1);
        /* example CSV:
        AirlineID,UniqueCarrier,Origin,Dest,DayOfWeek,DepDelay,ArrDelay
        20355,US,DCA,CLT,2,26.00,25.00
        20355,US,DCA,CLT,3,18.00,23.00
         */
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length != 7) {
                log.warn("unexpected field length: " + fields.length);
                return;
            }
            if (fields[0].equals("AirlineID")) {
                return;
            }
            word.set(fields[2]);
            context.write(word, one);
            word.set(fields[3]);
            context.write(word, one);
        }
    }

    public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text,IntWritable,Text,IntWritable>.Context context)
                throws IOException,InterruptedException {

            int sum=0;
            for(IntWritable x: values)
            {
                sum+=x.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf= new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf);
        job.setJarByClass(CountByAirport.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileStatus[] fss = fs.listStatus(new Path(args[0]));
        for (FileStatus fsvar : fss) {
            FileInputFormat.addInputPath(job, fsvar.getPath());
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
