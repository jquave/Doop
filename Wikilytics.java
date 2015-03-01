import java.io.IOException;
import java.util.StringTokenizer;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Wikilytics {

    public static class TokenizerMapper
            extends Mapper<Object, Text, LongWritable, Text> {

        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            int i = 0;
            long pageCount = -1;
            while (itr.hasMoreTokens()) {
                //word.set(itr.nextToken());
                //context.write(word, one);

                String iToken = itr.nextToken();

                if (i == 1) {
                    // URL
                    word.set(iToken);
                } else if (i == 3) {
                    // Page Count
                    pageCount = Long.parseLong(iToken);
                }

                //word.set(itr.nextToken());
                //word.set(key.toString());
                //context.write(word, one);
                //System.out.println("word " + word);

                i++;
            }

            context.write(new LongWritable(pageCount), word);

            //System.out.println("MapOut: " + word + ": " + pageCount);
            //System.out.println("\n=====\n");
        }
    }

    public static class WikiReducer
            extends Reducer<LongWritable, Text, Text, LongWritable> {
        private LongWritable result = new LongWritable();
        //private Text resultText = new Text();


        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            //int sum = 0;
            //String sumStr = "";

            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            //result.set(sum);
            //result.set(sumStr);
            //resultText.set(sumStr);

            result.set(sum);

            context.write(key, result);
            //context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Wikilytics.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(WikiReducer.class);
        job.setReducerClass(WikiReducer.class);

        //job.setSortComparatorClass(DescendingIntWritableComparable.class);
        //job.setSortComparatorClass(LongComparator.class);
        //job.setSortComparatorClass(DLongComparator.class);


        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        //job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
























