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
import java.util.StringTokenizer;

public class ItemClicker {
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
                if(isApril(word.toString())) {
                    Text text = new Text(getItemID(word.toString()));
                    context.write(text, one);
                }
            }
        }

        private Boolean isApril(String time) {
            String[] split = time.split(",");
            String[] ts = split[1].split("T");
            String[] t = ts[0].split("-");
            return t[1].equalsIgnoreCase("04");
        }

        private String getItemID(String text) {
            String[] split = text.split(",");
            return split[2];
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
        job.setJarByClass(ItemClicker.class);
        job.setMapperClass(ItemClicker.TokenizerMapper.class);
        job.setCombinerClass(ItemClicker.IntSumReducer.class);
        job.setReducerClass(ItemClicker.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("clicks.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/home/akriti/output2"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}