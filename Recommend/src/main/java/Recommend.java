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

public class Recommend {

    public static class Mapper1 extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] userItemScore = value.toString().split(",");
            int user = Integer.parseInt(userItemScore[0]);
            String item = userItemScore[1];
            String score = userItemScore[2];

            context.write(new IntWritable(user), new Text(item + "," + score));
        }
    }

    public static class Reducer1 extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("REDUCER1 KEY:" + key);
            StringBuilder userScoresList = new StringBuilder();
            for (Text value : values) {
                userScoresList.append(value.toString() + "_");
            }

            // to remove the trailing |
            userScoresList.setLength(userScoresList.length()-1);

            context.write(key,new Text(userScoresList.toString()));
        }
    }

    public static class Mapper2 extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("MAPPER2 KEY:" + key);
            System.out.println("MAPPER2 VALUE: " + value);

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

    public static class Reducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int cooccurenceMatrixSum = 0;
            for (IntWritable value : values) {
                cooccurenceMatrixSum += value.get();
            }
            context.write(key, new IntWritable(cooccurenceMatrixSum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "recommend part 1");

        job1.setJarByClass(Recommend.class);
        job1.setMapperClass(Mapper1.class);
        job1.setCombinerClass(Reducer1.class);
        job1.setReducerClass(Reducer1.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("temp_output1"));

        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "recommend part 2");

        job2.setJarByClass(Recommend.class);
        job2.setMapperClass(Mapper2.class);
        job2.setCombinerClass(Reducer2.class);
        job2.setReducerClass(Reducer2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path("temp_output1"));
        FileOutputFormat.setOutputPath(job2, new Path("temp_output2"));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
