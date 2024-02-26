import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class YouTubeDataAnalysis {

    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
        private boolean isHeader = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader || value.toString().equals("\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\"")) {
                isHeader = false;
                return;
            }

            String line = value.toString();
            String[] fields = line.split(",");

            if (fields.length > 5) {
                String subscribers = fields[3].trim();
                String avgViews = fields[5].trim();
                String avgLikes = fields[6].trim();
                String avgComments = fields[7].trim();

                processAndWrite(subscribers, context);
                processAndWrite(avgViews, context);
                processAndWrite(avgLikes, context);
                processAndWrite(avgComments, context);
            }
        }

        private void processAndWrite(String value, Context context) throws IOException, InterruptedException {
            // Trim leading and trailing whitespaces
            value = value.trim();

            if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
                // Remove quotes
                value = value.substring(1, value.length() - 1);
            }

            long count = 0;

            if (!value.isEmpty()) {
                if (value.endsWith("M")) {
                    value = value.substring(0, value.length() - 1); // remove 'M'
                } else if (value.endsWith("K")) {
                    value = value.substring(0, value.length() - 1); // remove 'K'
                }

                // Remove decimal point
                value = value.replace(".", "");

                count = Long.parseLong(value);

                String key = (count % 2 == 0) ? "Even" : "Odd";
                context.write(new Text(key), new LongWritable(count));
            }
        }
    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for (LongWritable value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "YouTube Data Analysis");
        job.setJarByClass(YouTubeDataAnalysis.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

