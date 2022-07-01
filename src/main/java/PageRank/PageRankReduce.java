package PageRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankReduce extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String name_list = "";
//            Text name_list = new Text();
        double PRvalue = 0;
        for (Text value : values) {
            if (String.valueOf(value).contains(",")) {
                name_list = String.valueOf(value);
            } else {
                PRvalue += Double.parseDouble(value.toString());
            }
        }
        context.write(key,
                new Text(1 - 0.85 + 0.85 * PRvalue + "#" + name_list));
    }
}
