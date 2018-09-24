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
import java.util.Iterator;
import java.util.StringTokenizer;

public class Clicker {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, LongWritable> {

        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                Text text = new Text(getTime(word.toString()));
                context.write(text, new LongWritable(getRevenue(word.toString())));
            }
        }

        private String getTime(String time) {
            String[] split = time.split(",");
            String[] ts = split[1].split("T");
            String[] t = ts[1].split(":");
            return t[0];
        }

        private Long getRevenue(String text) {
            String[] split = text.split(",");
            return Long.valueOf(split[3]) * Long.valueOf(split[4]);
        }
    }

    public static class LongSumReducer
            extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class SortMapper
            extends Mapper<Object, Text, LongWritable, Text> {

        public void map(Object key, Text value, Context context) throws  IOException, InterruptedException {

            StringTokenizer str = new StringTokenizer(value.toString());
            while(str.hasMoreTokens()) {
                Text t0 = new Text(str.nextToken());
                LongWritable t1 = new LongWritable(Long.parseLong(str.nextToken()));
                context.write(t1, t0);
            }


        }
    }

    public static  class SortReducer
            extends Reducer<LongWritable, Text, Text, LongWritable> {

        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws  IOException, InterruptedException {

            Iterator<Text> iter = values.iterator();
            while(iter.hasNext()) {
                Text t = iter.next();
                context.write(t, key);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "time revenue");
        job.setJarByClass(Clicker.class);
        job.setMapperClass(Clicker.TokenizerMapper.class);
        job.setCombinerClass(Clicker.LongSumReducer.class);
        job.setReducerClass(Clicker.LongSumReducer.class);
        job.setOutputKeyClass(Text.class);
        Path outputPath = new Path(args[1]);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        int jobStatus = job.waitForCompletion(true) ? 0:1;

        if (jobStatus == 0) {
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2);
            job2.setJarByClass(Clicker.class);
            job2.setMapperClass(Clicker.SortMapper.class);
            job2.setReducerClass(Clicker.SortReducer.class);
            // Sorts by frequency
            job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
            job2.setNumReduceTasks(1);
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Text.class);
            Path outputPath1 = new Path(args[2]);
            FileInputFormat.addInputPath(job2, outputPath);
            FileOutputFormat.setOutputPath(job2, outputPath1);
            outputPath1.getFileSystem(conf2).delete(outputPath1, true);
            System.exit(job2.waitForCompletion(true)?0:1);
        }


    }
}