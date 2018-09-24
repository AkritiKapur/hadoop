import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;



public class Success {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Transaction> {


        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);
        private Transaction buyTransaction = new Transaction(one, zero);
        private Transaction clickTransaction = new Transaction(zero, one);
        private Text word = new Text();

        //@Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                String line = word.toString();
                Text itemID = new Text(getItemID(line));
                if (isBuy(line)) {
                    context.write(itemID, buyTransaction);
                }
                else {
                    context.write(itemID, clickTransaction);
                }

            }
        }

        private Boolean isBuy(String text) {
            String[] split = text.split(",");
            return split.length == 5;
        }

        private String getItemID(String text) {
            String[] split = text.split(",");
            return split[2];
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Transaction, Text, FloatWritable> {
        private FloatWritable result = new FloatWritable();


        IntSumReducer() {
            System.out.println("in reducer");
        }


        //@Override
        public void reduce(Text key, Iterable<Transaction> values,
                           Context context
        ) throws IOException, InterruptedException {
            System.out.println("reduce func");
            int buy = 0;
            int click = 0;
            for (Transaction val : values) {
                buy += val.getBuy().get();
                click += val.getClick().get();
            }
            System.out.println("dsfkjsfd");
            if (click != 0) {
                result.set((float) buy / (float) click);
            } else {
                result.set(1);
            }
            context.write(key, result);
        }
    }


    public static class Transaction implements Writable {
        private IntWritable buy;
        private IntWritable click;

        Transaction(IntWritable buy, IntWritable click) {
            this.buy = buy;
            this.click = click;
        }

        public IntWritable getBuy() {
            return buy;
        }

        public IntWritable getClick() {
            return click;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            buy.write(dataOutput);
            click.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            buy.readFields(dataInput);
            click.readFields(dataInput);
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
        Job job = Job.getInstance(conf, "Success");
        job.setJarByClass(Success.class);

        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Transaction.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setMapperClass(Success.TokenizerMapper.class);
        //job.setCombinerClass(Success.IntSumReducer.class);
        job.setReducerClass(Success.IntSumReducer.class);
        //Path outputPath = new Path(args[1]);
        Path outputPath = new Path("/home/akriti/success");

        //FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path("input-datum/"));
        job.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0:1);

//        if (jobStatus == 0) {
//            Configuration conf2 = new Configuration();
//            Job job2 = Job.getInstance(conf2);
//            job2.setJarByClass(ItemClicker.class);
//            job2.setMapperClass(ItemClicker.SortMapper.class);
//            job2.setReducerClass(ItemClicker.SortReducer.class);
//            // Sorts by frequency
//            job2.setSortComparatorClass(ItemClicker.DescendingIntWritableComparable.DecreasingComparator.class);
//            job2.setNumReduceTasks(1);
//            job2.setOutputKeyClass(IntWritable.class);
//            job2.setOutputValueClass(Text.class);
//            Path outputPath1 = new Path(args[2]);
//            FileInputFormat.addInputPath(job2, outputPath);
//            FileOutputFormat.setOutputPath(job2, outputPath1);
//            outputPath1.getFileSystem(conf2).delete(outputPath1, true);
//            System.exit(job2.waitForCompletion(true)?0:1);
//        }


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