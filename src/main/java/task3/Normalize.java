package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;

public class Normalize {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Normalize");

        job.setJarByClass(Normalize.class);

        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) {
            String line = value.toString();
            String[] tokens = line.split(",");
            String name1 = tokens[0].substring(1);
            String name2 = tokens[1].substring(0, tokens[1].length() - 1);
            Text text1 = new Text(name1);
            Text text2 = new Text(name2 + ",1");
            try {
                context.write(text1, text2);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        Text lastname = null;
        HashMap<String, Integer> map = new HashMap<>();

        public void reducer(Text key, Text value, Context context) {
            if (lastname == null) {
                lastname = new Text(key);
            } else if (!lastname.equals(key)) {
                String s = "";

                s = s.substring(0, s.length() - 1);
                int sum = 0;
                for (String k : map.keySet()) {
                    sum += map.get(k);
                }
                for (String k : map.keySet()) {
                    s += k;
                    s += ",";
                    s += String.valueOf(((double) map.get(k)) / sum);
                    s += "|";
                }
                Text t = new Text(s);
                try {
                    context.write(lastname, t);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                lastname = new Text(key);
                map = new HashMap<>();
            }
            String svalue = value.toString();
            String[] tokens = svalue.split(",");
            if (map.containsKey(tokens[0])) {
                int n = map.get(tokens[0]);
                n += Integer.valueOf(tokens[1]);
                map.put(svalue, n);
            } else {
                map.put(svalue, Integer.valueOf(tokens[1]));
            }
        }

        public void cleanup(Context context) {
            if (!map.isEmpty()) {
                String s = "";
                int sum = 0;
                for (String k : map.keySet()) {
                    sum += map.get(k);
                }
                for (String k : map.keySet()) {
                    s += k;
                    s += ",";
                    s += String.valueOf(((double) map.get(k)) / sum);
                    s += "|";
                }
                s = s.substring(0, s.length() - 1);
                Text t = new Text(s);
                try {
                    context.write(lastname, t);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
