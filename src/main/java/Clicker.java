import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

public class Clicker {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        ClassLoader classLoader = getClass().getClassLoader();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                Text text = new Text(getTime(word.toString()));
                context.write(text, new IntWritable(getRevenue(word.toString())));
            }
        }

        private String getTime(String time) {
            String[] split = time.split(",");
            String[] ts = split[1].split("T");
            String[] t = ts[1].split(":");
            return t[0];
        }

        private Integer getRevenue(String text) {
            String[] split = text.split(",");
            return Integer.valueOf(split[3]) * Integer.valueOf(split[4]);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Clicker.class);
        job.setMapperClass(Clicker.TokenizerMapper.class);
        job.setCombinerClass(Clicker.IntSumReducer.class);
        job.setReducerClass(Clicker.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("buy.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/home/akriti/output1"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}