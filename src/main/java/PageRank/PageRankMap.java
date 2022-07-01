package PageRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankMap extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line_elements = value.toString().split("\\t");
        if (line_elements.length != 2) {
            System.err.print("There is a problem with the initial input in PageRank.");
            System.exit(2);
        }
        double PRvalue = Double.parseDouble(line_elements[1].split("#")[0]);
        String name_list = line_elements[1].split("#")[1];
        String[] temp = name_list.split("\\|");
        for (String name_pair : temp) {
            if(name_pair.length() > 0) {
                String name = name_pair.split(",")[0];
                double vote = Double.parseDouble(name_pair.split(",")[1]);
                context.write(new Text(name), new Text(String.valueOf(vote * PRvalue)));
            }
        }
        context.write(new Text(line_elements[0]), new Text(name_list));
    }
}
