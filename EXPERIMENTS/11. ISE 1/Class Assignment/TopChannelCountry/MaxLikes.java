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

public class MaxLikes {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    private static final int COUNTRY_INDEX = 4;
    private static final int CHANNEL_NAME_INDEX = 1;
    private static final int AVERAGE_LIKES_INDEX = 6;

    private boolean isHeader = true;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip header and empty lines
        if (isHeader || value.toString().trim().isEmpty()) {
            isHeader = false;
            return;
        }

        String line = value.toString();
        String[] fields = line.split(",");

        if (fields.length > AVERAGE_LIKES_INDEX) {
            String country = fields[COUNTRY_INDEX].trim();
            String channelName = fields[CHANNEL_NAME_INDEX].trim();
            String averageLikes = fields[AVERAGE_LIKES_INDEX].trim();

            // Simplified averageLikes conversion logic
            long averageLikesCount = 0;
            try {
                // Remove quotes and convert to a long
                averageLikesCount = Long.parseLong(averageLikes.replaceAll("\"", "").trim());
            } catch (NumberFormatException e) {
                // Handle the case where conversion fails
                e.printStackTrace(); // Log the exception for debugging
            }

            // Emit country as key and concatenate channelName with averageLikesCount as value
            context.write(new Text(country), new Text(channelName + "," + averageLikesCount));
        }
    }
}



    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            long maxLikes = 0;
            String maxChannel = "";

            for (Text value : values) {
                String[] fields = value.toString().split(",");
                String channelName = fields[0];
                long likes = Long.parseLong(fields[1]);

                if (likes > maxLikes) {
                    maxLikes = likes;
                    maxChannel = channelName;
                }
            }

            context.write(key, new Text(maxChannel + "," + maxLikes));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "MaxLikes");
        job.setJarByClass(MaxLikes.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

