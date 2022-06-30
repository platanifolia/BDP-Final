package task5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

public class LabelPropagation {
    public static void main(String[] args) throws Exception {
        String input = args[0];
        String output1 = args[1];
        String output2 = args[2];
        int iteration = 0;
        int iterationmax = 20;
        boolean status = false;
        while (iteration <= iterationmax) {
            if (iteration != 0) {
                String temp = output1;
                output1 = output2;
                output2 = temp;
            }
            Configuration conf = new Configuration();
            final FileSystem filesystem2 = FileSystem.get(new URI(output2), conf);
            final Path outPath = new Path(output2);
            if (filesystem2.exists(outPath)) {
                filesystem2.delete(outPath, true);
            }
            Job job = Job.getInstance(conf, "LabelPropagation");
            job.setJarByClass(LabelPropagation.class);

            final FileSystem filesystem1 = FileSystem.get(new URI(output1), conf);
            if (filesystem1.exists(new Path(output1))) {
                job.addCacheFile(new Path(output1 + "/part-r-00000").toUri());
            }

            FileInputFormat.addInputPath(job, new Path(input));

            job.setMapperClass(LabelPropagationMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            FileOutputFormat.setOutputPath(job, new Path(output2));
            status = job.waitForCompletion(true);
            iteration++;
        }
        System.exit(status ? 0 : 1);

    }

    public static class LabelPropagationMapper extends Mapper<LongWritable, Text, Text, Text> {
        HashMap<String, String> map = new HashMap<>();

        public void setup(Context context) {
            try {
                Path[] cacheFiles = context.getLocalCacheFiles();
                if (cacheFiles != null && cacheFiles.length > 0) {
                    String line;
                    BufferedReader joinReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
                    line = joinReader.readLine();
                    while (line != null) {
                        String[] tokens = line.split("\t");
                        map.put(tokens[0], tokens[1]);
                        line = joinReader.readLine();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void map(LongWritable key, Text value, Context context) {
            HashMap<String, Double> labelmap = new HashMap<>();
            String[] tokens1 = value.toString().split("\t");
            String[] tokens2 = tokens1[1].split("\\|");
            for (String s : tokens2) {
                String[] tokens3 = s.split(",");
                if (map.size() == 0) {
                    labelmap.put(tokens3[0], Double.valueOf(tokens3[1]));
                } else {
                    String label = map.get(tokens3[0]);
                    double n = Double.valueOf(tokens3[1]);
                    if (labelmap.containsKey(label)) {
                        n = n + labelmap.get(label);
                    }
                    labelmap.put(label, n);
                }
            }
            String label = tokens1[0];
            double n = 0;
            for (String s : labelmap.keySet()) {
                double sn = labelmap.get(s);
                if (sn > n) {
                    label = s;
                    n = sn;
                }
            }
            try {
                context.write(new Text(tokens1[0]), new Text(label));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
