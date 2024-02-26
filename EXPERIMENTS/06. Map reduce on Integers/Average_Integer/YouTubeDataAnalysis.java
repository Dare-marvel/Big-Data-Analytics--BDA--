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

                    context.write(new Text("subscribers"), new Text(subscribersCount.toString()));
                    context.write(new Text("avgViews"), new Text(avgViewsCount.toString()));
                    context.write(new Text("avgLikes"), new Text(avgLikesCount.toString()));
                    context.write(new Text("avgComments"), new Text(avgCommentsCount.toString()));
                    context.write(new Text("count"), new Text("1")); // Count of rows
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
        private BigDecimal sumSubscribers = BigDecimal.ZERO;
        private BigDecimal sumAvgViews = BigDecimal.ZERO;
        private BigDecimal sumAvgLikes = BigDecimal.ZERO;
        private BigDecimal sumAvgComments = BigDecimal.ZERO;
        private int rowCount = 0;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                BigDecimal currentValue = new BigDecimal(value.toString());

                if (key.toString().equals("subscribers")) {
                    sumSubscribers = sumSubscribers.add(currentValue);
                } else if (key.toString().equals("avgViews")) {
                    sumAvgViews = sumAvgViews.add(currentValue);
                } else if (key.toString().equals("avgLikes")) {
                    sumAvgLikes = sumAvgLikes.add(currentValue);
                } else if (key.toString().equals("avgComments")) {
                    sumAvgComments = sumAvgComments.add(currentValue);
                } else if (key.toString().equals("count")) {
                    rowCount += Integer.parseInt(value.toString());
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (rowCount > 0) {
                BigDecimal avgSubscribers = sumSubscribers.divide(new BigDecimal(rowCount), BigDecimal.ROUND_HALF_UP);
                BigDecimal avgAvgViews = sumAvgViews.divide(new BigDecimal(rowCount), BigDecimal.ROUND_HALF_UP);
                BigDecimal avgAvgLikes = sumAvgLikes.divide(new BigDecimal(rowCount), BigDecimal.ROUND_HALF_UP);
                BigDecimal avgAvgComments = sumAvgComments.divide(new BigDecimal(rowCount), BigDecimal.ROUND_HALF_UP);

                context.write(new Text("Average Subscribers: "), new Text(avgSubscribers.toString()));
                context.write(new Text("Average Avg Views: "), new Text(avgAvgViews.toString()));
                context.write(new Text("Average Avg Likes: "), new Text(avgAvgLikes.toString()));
                context.write(new Text("Average Avg Comments: "), new Text(avgAvgComments.toString()));
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

