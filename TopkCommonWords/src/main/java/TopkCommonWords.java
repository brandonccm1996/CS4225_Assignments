// Matric Number: A0172029J
// Name: Cheong Chee Mun Brandon
// TopkCommonWords.java

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopkCommonWords {

    public static class TopkCommonWordsMapper1 extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private BufferedReader fis;

        private URI[] stopwordFiles;
        private Path stopwordFilePath;
        private ArrayList<String> stopwordList = new ArrayList<>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            stopwordFiles = context.getCacheFiles();
            stopwordFilePath = new Path(stopwordFiles[0]);
            readStopwordFile(stopwordFilePath);

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String nextToken = itr.nextToken();
                if (!stopwordList.contains(nextToken)) {
                    word.set(nextToken);
                    context.write(word, one);
                }
            }
        }

        private void readStopwordFile(Path stopwordFile) {
            try {
                fis = new BufferedReader(new FileReader(stopwordFile.toString()));
                String stopword = null;
                while ((stopword = fis.readLine()) != null) {
                    stopwordList.add(stopword);
                }
            } catch (IOException e) {
                System.err.println("Exception while reading stop word file '" + stopwordFile + "' : " + e.toString());
            }
        }
    }

    public static class TopkCommonWordsReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class TopkCommonWordsMapper2 extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("MAP IN MAPPER2");
            System.out.println(value);
            context.write(new IntWritable(1), value);
        }
    }

    public static class TopkCommonWordsReducer2 extends Reducer<IntWritable, Text, IntWritable, Text> {
        private ArrayList<ImmutablePair<Integer, String>> allCommonWords = new ArrayList<>();

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("REDUCE IN REDUCER2");

            for (Text value : values) {
                System.out.println("BEFORE AGGREGATING: " + value.toString());
                String v[] = value.toString().split("\t");
                Integer count = Integer.parseInt(v[1]);
                ImmutablePair <Integer, String> countWordPair = new ImmutablePair<>(count, v[0]);
                allCommonWords.add(countWordPair);
            }

            Collections.sort(allCommonWords, new Comparator<ImmutablePair<Integer, String>>() {
                @Override
                public int compare(ImmutablePair<Integer, String> o1, ImmutablePair<Integer, String> o2) {
                    return o2.getKey().compareTo(o1.getKey());
                }
            });

            int numToOutput = Math.min(allCommonWords.size(), 20);
            for (int i = 0; i < numToOutput; i++) {
                context.write(new IntWritable(allCommonWords.get(i).getKey()), new Text(allCommonWords.get(i).getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "top k common words");

        job1.setJarByClass(TopkCommonWords.class);
        job1.setMapperClass(TopkCommonWordsMapper1.class);
        job1.setCombinerClass(TopkCommonWordsReducer1.class);
        job1.setReducerClass(TopkCommonWordsReducer1.class);
//        job1.setNumReduceTasks(1);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.addCacheFile(new Path(args[0] + "/stopwords.txt").toUri());
        FileInputFormat.addInputPath(job1, new Path(args[0] + "/task1-input1.txt"));
        FileInputFormat.addInputPath(job1, new Path(args[0] + "/task1-input2.txt"));
//        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path("temp_out1"));

//        System.exit(job1.waitForCompletion(true) ? 0 : 1);
        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "top k common words 2");
        job2.setJarByClass(TopkCommonWords.class);
        job2.setMapperClass(TopkCommonWordsMapper2.class);
//        job2.setCombinerClass(TopkCommonWordsReducer2.class);
        job2.setReducerClass(TopkCommonWordsReducer2.class);
        job2.setNumReduceTasks(1);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path("temp_out1"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}