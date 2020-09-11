import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
            System.out.println("ScoreMatrixGenerator_Reducer KEY:" + key);
            StringBuilder userScoresList = new StringBuilder();
            for (Text value : values) {
                userScoresList.append(value.toString() + "_");
            }

            // to remove the trailing |
            userScoresList.setLength(userScoresList.length()-1);

            context.write(key,new Text(userScoresList.toString()));
        }
    }

    public static class CooccurrenceMatrixGenerator_Mapper extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("CooccurrenceMatrixGenerator_Mapper KEY:" + key);
            System.out.println("CooccurrenceMatrixGenerator_Mapper VALUE: " + value);

            String[] userToItemScores = value.toString().split("\t");
            String itemScores = userToItemScores[1];

            String[] itemScoresList = itemScores.split("_");
            System.out.println(itemScoresList[0] + "[]" + itemScoresList[1]+ "[]" + itemScoresList[2]);
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

    public static class ScoreMatrixMultiply_Mapper extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("MAPPER3_1 KEY: " + key);
            System.out.println("MAPPER3_1 VALUE: " + value);

        }
    }

    public static class CooccurrenceMatrixMultiply_Mapper extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("MAPPER3_2 KEY: " + key);
            System.out.println("MAPPER3_2 VALUE: " + value);
        }
    }

    public static class Reducer3 extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

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
        FileOutputFormat.setOutputPath(job1, new Path("temp_output_score_matrix"));

        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "cooccurrence matrix generator");

        job2.setJarByClass(Recommend.class);
        job2.setMapperClass(CooccurrenceMatrixGenerator_Mapper.class);
        job2.setCombinerClass(CooccurrenceMatrixGenerator_Reducer.class);
        job2.setReducerClass(CooccurrenceMatrixGenerator_Reducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path("temp_output_score_matrix"));
        FileOutputFormat.setOutputPath(job2, new Path("temp_output_cooccurrence_matrix"));

        job2.waitForCompletion(true);

        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "matrix multiplication");

        job3.setJarByClass(Recommend.class);
        job3.setCombinerClass(Reducer3.class);
        job3.setReducerClass(Reducer3.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        MultipleInputs.addInputPath(job3, new Path("temp_output_score_matrix"), TextInputFormat.class, ScoreMatrixMultiply_Mapper.class);
        MultipleInputs.addInputPath(job3, new Path("temp_output_cooccurrence_matrix"), TextInputFormat.class, CooccurrenceMatrixMultiply_Mapper.class);
        FileOutputFormat.setOutputPath(job3, new Path("temp_output3"));

        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
