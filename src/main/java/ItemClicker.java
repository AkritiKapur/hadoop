import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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

    public static class SortMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context) throws  IOException, InterruptedException {

            StringTokenizer str = new StringTokenizer(value.toString());
            while(str.hasMoreTokens()) {
                Text t0 = new Text(str.nextToken());
                IntWritable t1 = new IntWritable(Integer.parseInt(str.nextToken()));
                context.write(t1, t0);
            }
        }
    }

    public static  class SortReducer
            extends Reducer<IntWritable, Text, Text, IntWritable> {

        int mCount = 0;

        // Source of finding top k - http://kamalnandan.com/hadoop/how-to-find-top-n-values-using-map-reduce/
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mCount = 0;
        }

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws  IOException, InterruptedException {

            if(mCount < 10) {
                for (Text t : values) {
                    context.write(t, key);
                    mCount ++;
                }
            }
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "item click frequency");
        job.setJarByClass(ItemClicker.class);
        job.setMapperClass(ItemClicker.TokenizerMapper.class);
        job.setCombinerClass(ItemClicker.IntSumReducer.class);
        job.setReducerClass(ItemClicker.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        int jobStatus = job.waitForCompletion(true) ? 0:1;

        if (jobStatus == 0) {
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2);
            job2.setJarByClass(ItemClicker.class);
            job2.setMapperClass(ItemClicker.SortMapper.class);
            job2.setReducerClass(ItemClicker.SortReducer.class);
            // Sorts by frequency
            job2.setSortComparatorClass(DescendingIntWritableComparable.DecreasingComparator.class);
            job2.setNumReduceTasks(1);
            job2.setOutputKeyClass(IntWritable.class);
            job2.setOutputValueClass(Text.class);
            Path outputPath1 = new Path(args[2]);
            FileInputFormat.addInputPath(job2, outputPath);
            FileOutputFormat.setOutputPath(job2, outputPath1);
            outputPath1.getFileSystem(conf2).delete(outputPath1, true);
            System.exit(job2.waitForCompletion(true)?0:1);
        }


    }

    // Source: https://stackoverflow.com/questions/18154686/how-to-implement-sort-in-hadoop
    //this class is defined outside of main not inside
    public static class DescendingIntWritableComparable extends IntWritable {
        /** A decreasing Comparator optimized for IntWritable. */
        public static class DecreasingComparator extends Comparator {
            public int compare(WritableComparable a, WritableComparable b) {
                return -super.compare(a, b);
            }
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                return -super.compare(b1, s1, l1, b2, s2, l2);
            }
        }
    }


}