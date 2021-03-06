// Matric Number: A0172029J
// Name: Cheong Chee Mun Brandon
// Recommend.java

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Recommend {

    public static class ScoreMatrixGenerator_Mapper extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] userItemScore = value.toString().split(",");
            int user = Integer.parseInt(userItemScore[0]);
            String item = userItemScore[1];
            String score = userItemScore[2];

            context.write(new IntWritable(user), new Text(item + "," + score));
        }
    }

    public static class ScoreMatrixGenerator_Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /*System.out.println("ScoreMatrixGenerator_Reducer KEY:" + key);*/
            StringBuilder userScoresList = new StringBuilder();
            for (Text value : values) {
                userScoresList.append(value.toString() + "_");
            }

            /*to remove the trailing _*/
            userScoresList.setLength(userScoresList.length()-1);

            context.write(key, new Text(userScoresList.toString()));
        }
    }

    public static class CooccurrenceMatrixGenerator_Mapper extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /*System.out.println("CooccurrenceMatrixGenerator_Mapper KEY:" + key);
             *             System.out.println("CooccurrenceMatrixGenerator_Mapper VALUE: " + value);*/

            String[] userToItemScores = value.toString().split("\t");
            String itemScores = userToItemScores[1];

            String[] itemScoresList = itemScores.split("_");
            for (int i = 0; i < itemScoresList.length; i++) {
                for (int j = 0; j < itemScoresList.length; j++) {
                    String[] itemScore1 = itemScoresList[i].split(",");
                    String item1 = itemScore1[0];

                    String[] itemScore2 = itemScoresList[j].split(",");
                    String item2 = itemScore2[0];

                    if (i == j) {
                        context.write(new Text(item1 + "," + item2), new IntWritable(0));
                    }
                    else{
                        context.write(new Text(item1 + "," + item2), new IntWritable(1));
                    }
                }
            }
        }
    }

    public static class CooccurrenceMatrixGenerator_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int cooccurrenceMatrixSum = 0;
            for (IntWritable value : values) {
                cooccurrenceMatrixSum += value.get();
            }
            context.write(key, new IntWritable(cooccurrenceMatrixSum));
        }
    }

    public static class ScoreMatrixMultiply_Mapper extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /*System.out.println("MAPPER3_1 KEY: " + key);
             *System.out.println("MAPPER3_1 VALUE: " + value);*/

            String[] userToItemScores = value.toString().split("\t");
            String user = userToItemScores[0];
            String itemScores = userToItemScores[1];

            String[] itemScoresList = itemScores.split("_");
            for (int i = 0; i < itemScoresList.length; i++) {
                String itemScore[] = itemScoresList[i].split(",");
                int item = Integer.parseInt(itemScore[0]);
                String score = itemScore[1];

                context.write(new IntWritable(item), new Text("SCORE\t" + user + "," + score));
            }
        }
    }

    public static class CooccurrenceMatrixMultiply_Mapper extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /*System.out.println("MAPPER3_2 KEY: " + key);
             *System.out.println("MAPPER3_2 VALUE: " + value);*/

            String[] itemsToCooccurrenceValue = value.toString().split("\t");
            String[] items = itemsToCooccurrenceValue[0].split(",");
            int item1 = Integer.parseInt(items[0]);
            String item2 = items[1];
            String cooccurrenceValue = itemsToCooccurrenceValue[1];

            context.write(new IntWritable(item1), new Text("COOC\t" + item2 + "," + cooccurrenceValue));
        }
    }

    public static class CooccurrenceMatrixMultiplyScoreMatrix_Reducer extends Reducer<IntWritable, Text, Text, DoubleWritable> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /*System.out.println("REDUCER3 KEY: " + key);*/

            Map<Integer, Double> userScoreMap = new HashMap<>();
            Map<Integer, Integer> itemCooccurrenceMap = new HashMap<>();

            for (Text value : values) {
                /*System.out.println("REDUCER3 VALUE: " + value);*/
                String[] valueString = value.toString().split("\t");
                String identifier = valueString[0];

                if (identifier.contains("SCORE")) {
                    String userScore[] = valueString[1].split(",");
                    int user = Integer.parseInt(userScore[0]);
                    double score = Double.parseDouble(userScore[1]);
                    userScoreMap.put(user, score);
                }
                else if (identifier.contains("COOC")) {
                    String itemCooccurrence[] = valueString[1].split(",");
                    int item = Integer.parseInt(itemCooccurrence[0]);
                    int coOccurrence = Integer.parseInt(itemCooccurrence[1]);
                    itemCooccurrenceMap.put(item, coOccurrence);
                }
            }

            for (Map.Entry<Integer, Double> userScoreMapElement : userScoreMap.entrySet()) {
                for (Map.Entry<Integer, Integer> itemCooccurrenceMapElement : itemCooccurrenceMap.entrySet()) {
                    int user = userScoreMapElement.getKey();
                    double score = userScoreMapElement.getValue();
                    int item = itemCooccurrenceMapElement.getKey();
                    int cooccurrence = itemCooccurrenceMapElement.getValue();
                    /*System.out.println("MAPPING: " + "user: " + user + " score: " + score + " item: " + item + " cooccurrence: " + cooccurrence);*/

                    context.write(new Text(user + "," + item), new DoubleWritable(score*cooccurrence));
                }
            }
        }
    }

    public static class MultiplicationResults_Mapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] userItemToMultiplicationResult = value.toString().split("\t");
            String userItem = userItemToMultiplicationResult[0];
            String multiplicationResult = userItemToMultiplicationResult[1];
            context.write(new Text(userItem), new Text(multiplicationResult));
        }
    }

    public static class UserScoresAlreadyGiven_Mapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] userItemScore = value.toString().split(",");
            String user = userItemScore[0];
            String item = userItemScore[1];
            String score = userItemScore[2];

            context.write(new Text(user + "," + item), new Text("UserGivenScore"));
        }
    }

    public static class SumResults_Reducer extends Reducer<Text, Text, IntWritable, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /*System.out.println("SumResults_Reducer");
             *System.out.println(key);*/
            String[] userItem = key.toString().split(",");
            int user = Integer.parseInt(userItem[0]);
            String item = userItem[1];

            boolean userAlreadyGivenScore = false;
            double sum = 0;

            for (Text value : values) {
                /*System.out.println(value);*/
                if (value.toString().contains("UserGivenScore")) {
                    userAlreadyGivenScore = true;
                    break;
                } else {
                    double multiplicationResult = Double.parseDouble(value.toString());
                    sum += multiplicationResult;
                }
            }

            if (!userAlreadyGivenScore) {
                context.write(new IntWritable(user), new Text(item + "," + sum));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "score matrix generator");

        job1.setJarByClass(Recommend.class);
        job1.setMapperClass(ScoreMatrixGenerator_Mapper.class);
        job1.setCombinerClass(ScoreMatrixGenerator_Reducer.class);
        job1.setReducerClass(ScoreMatrixGenerator_Reducer.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("recommendation/temp_output_score_matrix"));

        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "cooccurrence matrix generator");

        job2.setJarByClass(Recommend.class);
        job2.setMapperClass(CooccurrenceMatrixGenerator_Mapper.class);
        job2.setCombinerClass(CooccurrenceMatrixGenerator_Reducer.class);
        job2.setReducerClass(CooccurrenceMatrixGenerator_Reducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path("recommendation/temp_output_score_matrix"));
        FileOutputFormat.setOutputPath(job2, new Path("recommendation/temp_output_cooccurrence_matrix"));

        job2.waitForCompletion(true);

        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "matrix multiplication");

        job3.setJarByClass(Recommend.class);
        /*job3.setCombinerClass(Reducer3.class);*/
        job3.setReducerClass(CooccurrenceMatrixMultiplyScoreMatrix_Reducer.class);

        job3.setMapOutputKeyClass(IntWritable.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);
        MultipleInputs.addInputPath(job3, new Path("recommendation/temp_output_score_matrix"), TextInputFormat.class, ScoreMatrixMultiply_Mapper.class);
        MultipleInputs.addInputPath(job3, new Path("recommendation/temp_output_cooccurrence_matrix"), TextInputFormat.class, CooccurrenceMatrixMultiply_Mapper.class);
        FileOutputFormat.setOutputPath(job3, new Path("recommendation/temp_output_multiplication_results"));

        job3.waitForCompletion(true);

        Configuration conf4 = new Configuration();
        Job job4 = Job.getInstance(conf4, "sum up multiplication results");

        job4.setJarByClass(Recommend.class);
        job4.setMapperClass(MultiplicationResults_Mapper.class);
        /*job4.setCombinerClass(SumResults_Reducer.class);*/
        job4.setReducerClass(SumResults_Reducer.class);

        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(IntWritable.class);
        job4.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job4, new Path("recommendation/temp_output_multiplication_results"), TextInputFormat.class, MultiplicationResults_Mapper.class);
        MultipleInputs.addInputPath(job4, new Path(args[0]), TextInputFormat.class, UserScoresAlreadyGiven_Mapper.class);
        FileOutputFormat.setOutputPath(job4, new Path(args[1]));

        System.exit(job4.waitForCompletion(true) ? 0 : 1);
    }
}

/*
Note: a few temp_output directories will be created while running, but they have not been committed
to git because they are too large, so only the output directory is present in recommendation folder
 */