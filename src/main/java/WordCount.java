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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

//import org.apache.hadoop.io.IntWritable;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        ClassLoader classLoader = getClass().getClassLoader();


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            Map<String, Boolean> stopWords = getStopWords();
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                String sanitizedWord = getSanitizedWord(word.toString());
                if( !sanitizedWord.isEmpty() && !isStopWord(sanitizedWord, stopWords)) {
                    context.write(new Text(sanitizedWord), one);
                }
            }
        }

        private String getSanitizedWord(String word) {
            return word.replaceAll("[^A-Za-z]", "").toLowerCase();
        }

        private Boolean isStopWord(String word, Map<String, Boolean> stopWords) {
            return stopWords.getOrDefault(word, false);
        }

        private Map<String, Boolean> getStopWords() {
            //String fileName = "stopWords.txt";
            String fileName = "s3://wordcountamkap/datasets/stopwords";
            //File file = new File(classLoader.getResource(fileName).getFile());
            File file = new File(fileName);
            Map<String, Boolean> stopWords = new HashMap<>();

            // File reading code taken from https://www.mkyong.com/java8/java-8-stream-read-a-file-line-by-line/
            //read file into stream, try-with-resources
            try (Stream<String> lines = Files.lines(Paths.get(file.toURI()))) {
                lines.forEach(
                        line -> stopWords.put(line, true)
                );
            } catch (IOException e) {
                e.printStackTrace();
            }

            return stopWords;
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
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
//
//          System.out.println(value);

            StringTokenizer str = new StringTokenizer(value.toString());
//            System.out.println(str);
            while(str.hasMoreTokens()) {
                  Text t0 = new Text(str.nextToken());
//                System.out.println(t0.toString());
                System.out.println("fdkjgkd");
                  IntWritable t1 = new IntWritable(Integer.parseInt(str.nextToken()));
                  context.write(t1, t0);
            }


        }


    }

    public static class StringSort{

            private String name;
            public StringSort(String name) {
                this.name = name;
            }

            public String getName() {
                return this.name;
            }
            public static Comparator<StringSort> StuNameComparator = new Comparator<StringSort>() {


            public int compare(StringSort s1, StringSort s2) {
                String StudentName1 = s1.getName().toUpperCase();
                String StudentName2 = s2.getName().toUpperCase();

                //ascending order
                return StudentName1.compareTo(StudentName2);

                //descending order
                //return StudentName2.compareTo(StudentName1);
                 }
            };

        }

    public static  class SortReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws  IOException, InterruptedException {
            System.out.println();
            System.out.println(key);
            ArrayList<StringSort>  list = new ArrayList<StringSort>();
            Iterator<Text> iter = values.iterator();
            while(iter.hasNext()) {
                Text t = iter.next();
                list.add(new StringSort(t.toString()));
                //System.out.println(t);
                //context.write(key, t);
            }
            Collections.sort(list, StringSort.StuNameComparator);

            for(StringSort ss: list) {
                //System.out.println(ss.getName());
                context.write(key, new Text(ss.getName()));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        //System.out.println("Args 0 " + args[0] + " Args 1 " + args[1] + " Args 2 " + args[2] + " Args 3 " + args[3]);
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        int jobStatus = job.waitForCompletion(true) ? 0:1;
        if (jobStatus == 0) {

            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2);
            job2.setJarByClass(WordCount.class);
            job2.setMapperClass(SortMapper.class);
            job2.setReducerClass(SortReducer.class);
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