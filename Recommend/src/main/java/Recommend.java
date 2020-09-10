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
import java.util.StringTokenizer;

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
            System.out.println(key);
            StringBuilder userScoresList = new StringBuilder();
            for (Text value : values) {
                userScoresList.append(value.toString() + "|");
            }

            // to remove the trailing |
            userScoresList.setLength(userScoresList.length()-1);

            context.write(key,new Text(userScoresList.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "recommend part 1");

        job1.setJarByClass(Recommend.class);
        job1.setMapperClass(Mapper1.class);
        job1.setCombinerClass(Reducer1.class);
        job1.setReducerClass(Reducer1.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("temp_output1"));

        job1.waitForCompletion(true);

//        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}
