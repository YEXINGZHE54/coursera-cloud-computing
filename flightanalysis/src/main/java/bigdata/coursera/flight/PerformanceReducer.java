package bigdata.coursera.flight;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PerformanceReducer extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text,DoubleWritable,Text,DoubleWritable>.Context context)
            throws IOException,InterruptedException {

        double sum=0;
        double total = 0;
        for(DoubleWritable x: values)
        {
            sum+=x.get();
            total+=1;
        }
        result.set(100.00 * sum / total);
        context.write(key, result);
    }
}
