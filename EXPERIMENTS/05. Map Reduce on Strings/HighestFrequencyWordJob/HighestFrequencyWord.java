import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HighestFrequencyWord {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), ",");
      while (itr.hasMoreTokens()) {
        String token = itr.nextToken().replaceAll("^\"|\"$", "");
        if (!token.isEmpty()) {
          word.set(token);
          context.write(word, one);
        }
      }
    }
  }

  public static class MaxCountReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable maxCount = new IntWritable(Integer.MIN_VALUE);
    private Text maxCountWord = new Text();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      if (!key.toString().equals("") && sum > maxCount.get()) {
        maxCount.set(sum);
        maxCountWord.set(key);
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(maxCountWord, maxCount);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(HighestFrequencyWord.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(MaxCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

