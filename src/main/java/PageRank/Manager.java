package PageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Manager {
    public static void main(String[] args) throws Exception {
        Configuration conf0 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf0, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: PAGERANK <in> <out>");
            System.exit(2);
        }

        String input_path = otherArgs[0];
        String output_path = otherArgs[1] + "/init";

        Job job0 = Job.getInstance(conf0, "init");
        job0.setJarByClass(Manager.class);
        job0.setMapperClass(InputNormalization.class);
        job0.setNumReduceTasks(0);
        job0.setMapOutputKeyClass(Text.class);
        job0.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job0, new Path(input_path));
        FileOutputFormat.setOutputPath(job0, new Path(output_path));
        job0.waitForCompletion(true);
        input_path = output_path;

        boolean status;
        for(int i = 0; i < 10; i++){
            output_path = otherArgs[1] + "/iterate" + "/" + i;
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "RageRank iterate" + i);
            job.setJarByClass(Manager.class);
            job.setMapperClass(PageRankMap.class);
            job.setMapOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setReducerClass(PageRankReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(input_path));
            FileOutputFormat.setOutputPath(job, new Path(output_path));

            job.waitForCompletion(true);
            input_path = output_path;
        }
        output_path = otherArgs[1] + "/final";
        Configuration conf1 = new Configuration();
        Job job = Job.getInstance(conf1, "Sort");
        job.setJarByClass(Manager.class);
        job.setMapperClass(SortMap.class);
        job.setSortComparatorClass(Descend.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(SortReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));

        status = job.waitForCompletion(true);

        System.exit(status ? 0 : 1);
    }
}
