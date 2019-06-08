package bigdata.coursera.flight;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class RankCarrierByDep extends Mapper<LongWritable, Text,Text,DoubleWritable> {
    public static final Log log = LogFactory.getLog(RankCarrierByDep.class);
    private Text word = new Text();
    private final static DoubleWritable one = new DoubleWritable(1);
    private final static DoubleWritable zero = new DoubleWritable(0);
    private static final String OriginKey = "bigdata.flights.origin";
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
        String origin = context.getConfiguration().get(OriginKey, "");
        if (!fields[2].equals(origin)) {
            return; //not depart from X, just skip
        }
        word.set(fields[1]);
        double delay = Double.parseDouble(fields[5]);
        if (delay >= 0.01) {
            context.write(word, zero);
        } else {
            context.write(word, one);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf= new Configuration();
        conf.set(OriginKey, args[2]);
        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf);
        job.setJarByClass(RankCarrierByDep.class);
        job.setMapperClass(RankCarrierByDep.class);
        job.setReducerClass(PerformanceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
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
