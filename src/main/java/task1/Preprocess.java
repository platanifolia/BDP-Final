package task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;

public class Preprocess {
    public static void main(String[] args) {
        String namelist = args[0];
        String input = args[1];
        String output = args[2];
        boolean status = false;
        Configuration conf = new Configuration();
        Job job = null;
        try {
            job = Job.getInstance(conf, "Preprocess");
        } catch (IOException e) {
            e.printStackTrace();
        }
        job.setJarByClass(Preprocess.class);

        job.addCacheFile(new Path(namelist).toUri());

        try {
            FileInputFormat.addInputPath(job, new Path(input));
        } catch (IOException e) {
            e.printStackTrace();
        }

        job.setMapperClass(PreprocessMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(PreprocessReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(output));
        try {
            status = job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.exit(status ? 0 : 1);
    }

    public static class PreprocessMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        HashSet<String> namelist = new HashSet<>();

        public void setup(Context context) {
            try {
                URI[] cacheFiles = context.getCacheFiles();
                if (cacheFiles != null && cacheFiles.length > 0) {
                    String name;
                    BufferedReader joinReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
                    name = joinReader.readLine();
                    while (name != null) {
                        namelist.add(name);
                        name = joinReader.readLine();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void map(LongWritable key, Text value, Context context) {
            String line = value.toString();
            int length = line.length();
            String r = "";
            for (int i = 0; i < length - 1; i++) {
                for (int j = i + 1; j <= Math.min(length, i + 8); j++) {
                    String subline = line.substring(i, j);
                    if (namelist.contains(subline)) {
                        if (i != 0 && subline.equals("玄奘")) {
                            if (line.charAt(i - 1) == '陈') {
                                continue;
                            }
                        } else if (i != 0 && subline.equals("悟空")) {
                            if (line.charAt(i - 1) == '孙') {
                                continue;
                            }
                        } else if (i != 0 && subline.equals("猴王")) {
                            if (line.charAt(i - 1) == '美') {
                                continue;
                            }
                        } else if (i != 0 && subline.equals("悟能")) {
                            if (line.charAt(i - 1) == '猪') {
                                continue;
                            }
                        } else if (i != 0 && subline.equals("八戒")) {
                            if (line.charAt(i - 1) == '猪') {
                                continue;
                            }
                        } else if (i != 0 && subline.equals("悟净")) {
                            if (line.charAt(i - 1) == '沙') {
                                continue;
                            }
                        }
                        if (i != length - 2 && subline.equals("江流")) {
                            if (line.charAt(i + 2) == '儿') {
                                continue;
                            }
                        } else if (i != length - 2 && subline.equals("如来")) {
                            if (line.startsWith("佛祖", i + 2)) {
                                continue;
                            }
                        } else if (i != length - 2 && subline.equals("观音")) {
                            if (line.startsWith("菩萨", i + 2)) {
                                continue;
                            }
                        }
                        if (i != length - 3 && subline.equals("观世音")) {
                            if (line.startsWith("菩萨", i + 3)) {
                                continue;
                            }
                        }
                        r += subline;
                        r += " ";
                    }
                }
            }
            if (r != "") {
                try {
                    context.write(NullWritable.get(), new Text(r));
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class PreprocessReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        public void reduce(NullWritable key, Iterable<Text> values, Context context) {
            try {
                for (Text value : values) {
                    context.write(key, value);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
