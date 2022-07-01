package PageRank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMap extends Mapper<Object, Text, DoubleWritable, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String [] temp1 = value.toString().split("\\t");
        double PRvalue = Double.parseDouble(temp1[1].split("#")[0]);
        context.write(new DoubleWritable(PRvalue), new Text(temp1[0]));
    }
}
