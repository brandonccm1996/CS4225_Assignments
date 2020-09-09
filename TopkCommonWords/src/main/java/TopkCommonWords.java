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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopkCommonWords {

    public static class TopkCommonWordsMapper1 extends Mapper<Object, Text, Text, IntWritable> {
        private Map<String, Integer> hMap = new HashMap<>();
        private ArrayList<String> stopwordList = new ArrayList<>();

        public void map(Object key, Text value, Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            Path stopwordFilePath = new Path(cacheFiles[0]);
            readStopwordFile(stopwordFilePath);

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String nextToken = itr.nextToken();
                if (!stopwordList.contains(nextToken)) {
                    if (hMap.containsKey(nextToken)) {
                        int prevCount = hMap.get(nextToken);
                        hMap.put(nextToken, prevCount+1);
                    }
                    else {
                        hMap.put(nextToken, 1);
                    }
                }
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> entry : hMap.entrySet()) {
                String hMapKey = entry.getKey();
                context.write(new Text(hMapKey), new IntWritable(entry.getValue()));
            }
        }

        private void readStopwordFile(Path stopwordFile) {
            try {
                BufferedReader fis = new BufferedReader(new FileReader(stopwordFile.toString()));
                String stopword = null;
                while ((stopword = fis.readLine()) != null) {
                    stopwordList.add(stopword);
                }
            } catch (IOException e) {
                System.err.println("Exception while reading stop word file '" + stopwordFile + "' : " + e.toString());
            }
        }
    }

    public static class TopkCommonWordsMapper2 extends Mapper<Object, Text, Text, IntWritable> {
        private Map<String, Integer> hMap = new HashMap<>();
        private ArrayList<String> stopwordList = new ArrayList<>();

        public void map(Object key, Text value, Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            Path stopwordFilePath = new Path(cacheFiles[0]);
            readStopwordFile(stopwordFilePath);

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String nextToken = itr.nextToken();
                if (!stopwordList.contains(nextToken)) {
                    if (hMap.containsKey(nextToken)) {
                        int prevCount = hMap.get(nextToken);
                        hMap.put(nextToken, prevCount+1);
                    }
                    else {
                        hMap.put(nextToken, 1);
                    }
                }
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> entry : hMap.entrySet()) {
                String hMapKey = entry.getKey();
                context.write(new Text(hMapKey), new IntWritable(entry.getValue()));
            }
        }

        private void readStopwordFile(Path stopwordFile) {
            try {
                BufferedReader fis = new BufferedReader(new FileReader(stopwordFile.toString()));
                String stopword = null;
                while ((stopword = fis.readLine()) != null) {
                    stopwordList.add(stopword);
                }
            } catch (IOException e) {
                System.err.println("Exception while reading stop word file '" + stopwordFile + "' : " + e.toString());
            }
        }
    }

    public static class TopkCommonWordsReducer1 extends Reducer<Text, IntWritable, IntWritable, Text> {
        private Map<String, Integer> hMap = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int file1count = 0;
            int file2count = 0;

            for (IntWritable value : values) {
                if (file1count == 0) {
                    file1count = value.get();
                } else {
                    file2count = value.get();
                }
            }
            int countToOutput = Math.min(file1count, file2count);
            hMap.put(key.toString(), countToOutput);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            ArrayList<ImmutablePair<Integer, String>> allCommonWords = new ArrayList<>();

            for (Map.Entry<String, Integer> entry : hMap.entrySet()) {
                ImmutablePair <Integer, String> wordCountPair = new ImmutablePair<>(entry.getValue(), entry.getKey());
                allCommonWords.add(wordCountPair);
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
        job1.setReducerClass(TopkCommonWordsReducer1.class);
        job1.setNumReduceTasks(1);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        job1.addCacheFile(new Path(args[0] + "/stopwords.txt").toUri());
        MultipleInputs.addInputPath(job1, new Path(args[0] + "/task1-input1.txt"), TextInputFormat.class, TopkCommonWordsMapper1.class);
        MultipleInputs.addInputPath(job1, new Path(args[0] + "/task1-input2.txt"), TextInputFormat.class, TopkCommonWordsMapper2.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}
