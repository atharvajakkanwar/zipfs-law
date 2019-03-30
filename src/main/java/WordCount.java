import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private HashSet<String> chars = new HashSet<>(Arrays.asList("abcdefghijklmnopqrstuvwxyz".split("")));
        private Map<String, Integer> map;

        private final IntWritable totalCount = new IntWritable();
        private final Text finalWord = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            this.map = new HashMap<>();
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String currWord = itr.nextToken().toLowerCase();
                if (chars.contains(Character.toString(currWord.charAt(0)))) {
                    if (map.containsKey(currWord)){
                        map.put(currWord, map.get(currWord)+1);
                    } else {
                        map.put(currWord, 1);
                    }
                }
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String s : map.keySet()) {
                finalWord.set(s);
                totalCount.set(map.get(s));
                context.write(finalWord, totalCount);
            }
            super.cleanup(context);
        }
    }


    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
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

    public static void main(String[] args) throws Exception {
        long x = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(26);
        job.waitForCompletion(true);
        System.out.println("Total time elapsed: " + (System.currentTimeMillis() - x) + "ms");
    }
}
