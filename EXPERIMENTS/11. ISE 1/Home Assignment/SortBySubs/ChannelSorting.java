import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.NullWritable;

public class ChannelSorting {

    public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
    private boolean isHeader = true;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(isHeader || value.toString().equals("\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\"")){
            isHeader = false;
            return;
        }
        String line = value.toString();
        String[] fields = line.split(",");
        if (fields.length > 3) {
            String country = fields[4].trim();
            String subscribers = fields[3].trim();
            if (subscribers.length() >= 3) {
                subscribers = subscribers.substring(1, subscribers.length() - 2); // remove quotes
            }
            long subscriberCount = 0;
            if (subscribers.endsWith("M")) {
                subscribers = subscribers.substring(0, subscribers.length() - 1); // remove 'M'
                subscriberCount = (long) (Double.parseDouble(subscribers) * 1000000); // convert to actual count
            } else if (subscribers.endsWith("K")) {
                subscribers = subscribers.substring(0, subscribers.length() - 1); // remove 'K'
                subscriberCount = (long) (Double.parseDouble(subscribers) * 1000); // convert to actual count
            }
            context.write(new LongWritable(subscriberCount), new Text(line));
        }
    }
}



    public static class Reduce extends Reducer<LongWritable, Text, NullWritable, Text> {
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(NullWritable.get(), val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "channel sorting");
        job.setJarByClass(ChannelSorting.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setSortComparatorClass(LongWritable.DecreasingComparator.class); // sort in descending order
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

