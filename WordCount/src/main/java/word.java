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

public class word {

    private static Configuration entries;
    private static Job word_count;

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private StringTokenizer stringTokenizer;

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
            stringTokenizer = new StringTokenizer(value.toString());
            while (stringTokenizer.hasMoreTokens()) {
                word.set(stringTokenizer.nextToken());
                context.write(word, one);

            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
            int sum = 0;
            for (IntWritable val : values
                    ) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        entries = new Configuration();
        word_count = Job.getInstance(entries, "word count");
        word_count.setJarByClass(word.class);
        word_count.setMapperClass(TokenizerMapper.class);
        word_count.setCombinerClass(IntSumReducer.class);
        word_count.setReducerClass(IntSumReducer.class);
        word_count.setOutputKeyClass(Text.class);
        word_count.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(word_count,new Path("hdfs://localhost:9000/park"));
        FileOutputFormat.setOutputPath(word_count,new Path("hdfs://localhost:9000/park/result"));
        System.exit(word_count.waitForCompletion(true)?0: 1);
    }
}
