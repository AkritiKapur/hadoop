import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.StringTokenizer;
import java.util.Iterator;



public class Success {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

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
                    context.write(itemID, new Text("1:0"));
                }
                else {
                    context.write(itemID, new Text("0:1"));
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
            extends Reducer<Text, Text, Text, FloatWritable> {
        private FloatWritable result = new FloatWritable();


        //@Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int buy = 0;
            int click = 0;
            for (Text val : values) {
                String[] transaction = val.toString().split(":");
                buy += Integer.parseInt(transaction[0]);
                click += Integer.parseInt(transaction[1]);
            }
            if (click != 0) {
                result.set((float) buy / (float) click);
            } else {
                result.set(1);
            }
            context.write(key, result);
        }
    }


    public static class SortMapper
            extends Mapper<Object, Text, FloatWritable, LongWritable> {

        public void map(Object key, Text value, Context context) throws  IOException, InterruptedException {

            StringTokenizer str = new StringTokenizer(value.toString());
            while(str.hasMoreTokens()) {
                LongWritable t0 = new LongWritable(Long.parseLong(str.nextToken()));
                FloatWritable t1 = new FloatWritable(Float.parseFloat(str.nextToken()));
                context.write(t1, t0);
            }
        }
    }

    public static  class SortReducer
            extends Reducer<FloatWritable, LongWritable, LongWritable, FloatWritable> {

        int mCount = 1;

        // Source of finding top k - http://kamalnandan.com/hadoop/how-to-find-top-n-values-using-map-reduce/
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mCount = 1;
        }

        public static Comparator<LongWritable> LongWComparator = new Comparator<LongWritable>() {


            public int compare(LongWritable l1, LongWritable l2) {
                //String StudentName1 = s1.getName().toUpperCase();
                //String StudentName2 = s2.getName().toUpperCase();

                //ascending order
                return l2.compareTo(l1);

                //descending order
                //return StudentName2.compareTo(StudentName1);
            }
        };

        public void reduce(FloatWritable key, Iterable<LongWritable> values, Context context)
                throws  IOException, InterruptedException {

//            if(mCount > 10) {
//                for (Text t : values) {
//                    context.write(t, key);
//                    mCount ++;
//                }
//            }

            System.out.println(key);
            ArrayList<LongWritable>  lwl = new ArrayList<LongWritable>();
            Iterator<LongWritable> iter = values.iterator();
            while(iter.hasNext()) {
                //LongWritable lw = iter.next();
                //System.out.println();
                lwl.add(new LongWritable(iter.next().get()));
            }


            for(LongWritable l: lwl) {
                System.out.println(l);
                //System.out.println(val);
            }

            Collections.sort(lwl);


            for(LongWritable l: lwl) {
                if (mCount >10) {
                    break;
                }
                //System.out.println(l);
                context.write(l, key);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Success");
        job.setJarByClass(Success.class);

        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
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
        int jobStatus = job.waitForCompletion(true) ? 0:1;

        if (jobStatus == 0) {
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2);
            job2.setJarByClass(Success.class);
            job2.setMapperClass(Success.SortMapper.class);
            job2.setReducerClass(Success.SortReducer.class);
            // Sorts by frequency
            job2.setSortComparatorClass(Success.DescendingFloatWritableComparable.DecreasingComparator.class);
            job2.setMapOutputValueClass(LongWritable.class);
            job2.setMapOutputKeyClass(FloatWritable.class);
            job2.setNumReduceTasks(1);
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(FloatWritable.class);
            Path outputPath1 = new Path("/home/akriti/success=final");
            FileInputFormat.addInputPath(job2, outputPath);
            FileOutputFormat.setOutputPath(job2, outputPath1);
            outputPath1.getFileSystem(conf2).delete(outputPath1, true);
            System.exit(job2.waitForCompletion(true)?0:1);
        }


    }

    // Source: https://stackoverflow.com/questions/18154686/how-to-implement-sort-in-hadoop
    //this class is defined outside of main not inside
    public static class DescendingFloatWritableComparable extends FloatWritable {
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