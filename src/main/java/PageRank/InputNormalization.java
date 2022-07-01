package PageRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class InputNormalization extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line_element = value.toString().split("\\t");
        if (line_element.length != 2) {
            System.err.print("There is a problem with the initial input.");
            System.exit(1);
        }
        context.write(new Text(line_element[0]), new Text("1#" + line_element[1]));
    }
}
