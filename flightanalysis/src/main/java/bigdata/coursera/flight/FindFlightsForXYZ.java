package bigdata.coursera.flight;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class FindFlightsForXYZ extends Mapper<LongWritable, Text,Text,Text> {
    public static final Log log = LogFactory.getLog(FindFlightsForXYZ.class);
    private Text word = new Text();
    private static final String OriginKey = "bigdata.flights.origin";
    private static final String MiddleKey = "bigdata.flights.middle";
    private static final String DestKey = "bigdata.flights.dest";
    private static final String OriginDateKey = "bigdata.flights.date.origin";
    private static final String MiddleDateKey = "bigdata.flights.date.middle";
    /* example CSV:
    AirlineID,UniqueCarrier,Origin,Dest,DayOfWeek,DepDelay,ArrDelay,FlightDate,DepTime,ArrTime
    */
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields[0].equals("AirlineID")) {
            return;
        }
        String origin = context.getConfiguration().get(OriginKey, "");
        String middle = context.getConfiguration().get(MiddleKey, "");
        String dest = context.getConfiguration().get(DestKey, "");
        String originDate = context.getConfiguration().get(OriginDateKey, "");
        String middleDate = context.getConfiguration().get(MiddleDateKey, "");

        // filter origin flight
        if (fields[7].equals(originDate) && fields[2].equals(origin) && fields[3].equals(middle)) {
            Double delay = Double.parseDouble(fields[5]);
            if (delay <= 0.01) {
                delay = 0.00;
            }
            SimpleDateFormat sdf = new SimpleDateFormat("HHmm");
            Calendar c = Calendar.getInstance();
            try {
                c.setTime(sdf.parse(fields[8]));
            } catch (ParseException e) {
                log.warn("invalid time format:" + e.getMessage());
                return;
            }
            c.add(Calendar.MINUTE, 0 - delay.intValue());
            if (c.get(Calendar.HOUR) >= 12) {
                return;
            }

        }
        // filter middle flight
        if (fields[7].equals(middleDate) && fields[2].equals(middle) && fields[3].equals(dest)) {
            Double delay = Double.parseDouble(fields[5]);
            if (delay <= 0.01) {
                delay = 0.00;
            }
            SimpleDateFormat sdf = new SimpleDateFormat("HHmm");
            Calendar c = Calendar.getInstance();
            try {
                c.setTime(sdf.parse(fields[8]));
            } catch (ParseException e) {
                log.warn("invalid time format:" + e.getMessage());
                return;
            }
            c.add(Calendar.MINUTE, 0 - delay.intValue());
            if (c.get(Calendar.HOUR) < 12) {
                return;
            }
        }
        word.set(fields[2]);
        context.write(word, value);
    }

    public static class Reduce extends Reducer<Text, Text,Text,Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Reducer<Text,Text,Text,Text>.Context context)
                throws IOException,InterruptedException {
            String origin = context.getConfiguration().get(OriginKey, "");
            String middle = context.getConfiguration().get(MiddleKey, "");
            if (key.toString().equals(origin) || key.toString().equals(middle)) {
                Map<String, Double> flights = new HashMap<>();
                for (Text value : values) {
                    String[] fields = value.toString().split(",");
                    String flight = fields[0];
                    if (null == fields[6] || fields[6].length() == 0) {
                        continue;
                    }
                    Double delay = Double.parseDouble(fields[6]);
                    if (delay <= 0.01) {
                        delay = 0.00;
                    }
                    flights.put(flight, delay);
                }
                List<Map.Entry<String, Double>> items = new ArrayList<>(flights.entrySet());
                Collections.sort(items, new Comparator<Map.Entry<String, Double>>(){
                    @Override
                    public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                        if (o1.getValue() > o2.getValue()) {
                            return 1;
                        } else if (o1.getValue() == o2.getValue()) {
                            return 0;
                        }
                        return -1;
                    }
                });
                if (items.size() > 0) {
                    result.set(items.get(0).getKey());
                }
            } else {
                return;
            }
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat dataSdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar c = Calendar.getInstance();
        c.setTime(sdf.parse(args[5]));
        Date origin = c.getTime();
        c.add(Calendar.DAY_OF_MONTH, 2);
        Date middle = c.getTime();
        Configuration conf= new Configuration();
        conf.set(OriginKey, args[2]);
        conf.set(MiddleKey, args[3]);
        conf.set(DestKey, args[4]);
        conf.set(OriginDateKey, dataSdf.format(origin));
        conf.set(MiddleDateKey, dataSdf.format(middle));
        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf);
        job.setJarByClass(FindFlightsForXYZ.class);
        job.setMapperClass(FindFlightsForXYZ.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
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
