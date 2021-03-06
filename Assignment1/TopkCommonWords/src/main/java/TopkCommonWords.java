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

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Map<String, Integer> hMap = new HashMap<>();
        private ArrayList<String> stopwordList = new ArrayList<>();

        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            URI[] cacheFiles = Job.getInstance(conf).getCacheFiles();
            Path stopwordFilePath = new Path(cacheFiles[0].getPath());
            String stopwordFileName = stopwordFilePath.getName();
            readStopwordFile(stopwordFileName);
        }

        public void map(Object key, Text value, Context context) throws IOException {
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
                String word = entry.getKey();
                int count = entry.getValue();
                context.write(new Text(word), new IntWritable(count));
            }
        }

        private void readStopwordFile(String stopwordFileName) {
            try {
                BufferedReader fis = new BufferedReader(new FileReader(stopwordFileName));
                String stopword = null;
                while ((stopword = fis.readLine()) != null) {
                    stopwordList.add(stopword);
                }
            } catch (IOException e) {
                System.err.println("Exception while reading stop word file");
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, IntWritable, Text> {
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
            if (countToOutput > 0) {
                hMap.put(key.toString(), countToOutput);
            }
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
                int countOfCommonWord = allCommonWords.get(i).getKey();
                String commonWord = allCommonWords.get(i).getValue();
                context.write(new IntWritable(countOfCommonWord), new Text(commonWord));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "top k common words");

        job.setJarByClass(TopkCommonWords.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new Path(args[2]).toUri());
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TokenizerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TokenizerMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

/*
Note:
Not sure why this TopkCommonWords.java file works correctly on NUS SOC cluster, but not on my local
Windows machine. On my local Windows machine, it will encouter exception while reading
stopwords.txt thus stopwords are not being removed from the output.

The cm_output in the commonwords folder is the wrong output that will be obtained when running on
my local Windows machine. The correct output should be in the answers.txt file.

To make it run correctly on my local Windows machine, I have to change setup function and
readStopwordFile function to the following:

public void setup(Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    URI[] cacheFiles = Job.getInstance(conf).getCacheFiles();
    Path stopwordFilePath = new Path(cacheFiles[0].getPath());
    readStopwordFile(stopwordFilePath);
}

private void readStopwordFile(Path stopwordFile) {
    try {
        BufferedReader fis = new BufferedReader(new FileReader(stopwordFile.toString()));
        String stopword = null;
        while ((stopword = fis.readLine()) != null) {
            stopwordList.add(stopword);
        }
    } catch (IOException e) {
        System.err.println("Exception while reading stop word file");
    }
}
 */