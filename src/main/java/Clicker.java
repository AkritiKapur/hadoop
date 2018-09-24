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
import java.util.StringTokenizer;

public class Clicker {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, LongWritable> {

        private Text word = new Text();
        ClassLoader classLoader = getClass().getClassLoader();

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

        private Integer getRevenue(String text) {
            String[] split = text.split(",");
            return Integer.valueOf(split[3]) * Integer.valueOf(split[4]);
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

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Clicker.class);
        job.setMapperClass(Clicker.TokenizerMapper.class);
        job.setCombinerClass(Clicker.LongSumReducer.class);
        job.setReducerClass(Clicker.LongSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path("buy.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/home/akriti/output1"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}