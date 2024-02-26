import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;


import java.io.IOException;
import java.math.BigDecimal;

public class YouTubeDataAnalysis {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private boolean isHeader = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader || value.toString().equals("\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\"")) {
                isHeader = false;
                return;
            }

            String line = value.toString();
            String[] fields = line.split(",");

            if (fields.length > 7) { // Ensure at least 8 fields
                String subscribers = fields[3].trim(); // Subscribers column
                String avgViews = fields[5].trim();
                String avgLikes = fields[6].trim();
                String avgComments = fields[7].trim();

                if (!subscribers.isEmpty() && !avgViews.isEmpty() && !avgLikes.isEmpty() && !avgComments.isEmpty() &&
                        !subscribers.equals("\"\"\"\"") && !avgViews.equals("\"\"\"\"") && !avgLikes.equals("\"\"\"\"") && !avgComments.equals("\"\"\"\"")) {
                    BigDecimal subscribersCount = convertToActual(subscribers);
                    BigDecimal avgViewsCount = convertToActual(avgViews);
                    BigDecimal avgLikesCount = convertToActual(avgLikes);
                    BigDecimal avgCommentsCount = convertToActual(avgComments);

                    BigDecimal maxCount = subscribersCount.max(avgViewsCount).max(avgLikesCount).max(avgCommentsCount);

                    context.write(new Text("max"), new Text(maxCount.toString()));
                }
            }
        }

        private BigDecimal convertToActual(String value) {
            value = value.replaceAll("\"", "").trim();

            if (value.isEmpty() || value.equals("\"\"\"\"")) {
                return BigDecimal.ZERO;
            }

            BigDecimal count = BigDecimal.ZERO;

            if (value.endsWith("M")) {
                value = value.substring(0, value.length() - 1); // remove 'M'
                count = new BigDecimal(value).multiply(new BigDecimal("1000000"));
            } else if (value.endsWith("K")) {
                value = value.substring(0, value.length() - 1); // remove 'K'
                count = new BigDecimal(value).multiply(new BigDecimal("1000"));
            } else {
                count = new BigDecimal(value);
            }

            return count;
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text resultKey = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            BigDecimal maxCount = BigDecimal.ZERO;

            for (Text value : values) {
                BigDecimal currentCount = new BigDecimal(value.toString());
                maxCount = maxCount.max(currentCount);
            }

            resultKey.set("Largest Integer: " + maxCount.toString());
            context.write(resultKey, new Text(""));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "YouTube Data Analysis");
        job.setJarByClass(YouTubeDataAnalysis.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (job.waitForCompletion(true)) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}

