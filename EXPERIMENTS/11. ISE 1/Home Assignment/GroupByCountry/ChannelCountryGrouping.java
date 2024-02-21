import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChannelCountryGrouping {

    public static class ChannelMapper extends Mapper<Object, Text, Text, Text> {

        private Text country = new Text();
        private Text channelInfo = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (((Text) value).toString().trim().isEmpty()) {
                return; // Ignore empty lines
            }

            String[] tokens = value.toString().split(",");
            if (tokens.length >= 5) {
                country.set(tokens[4].trim());
                channelInfo.set(value);
                context.write(country, channelInfo);
            }
        }
    }

    public static class ChannelReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Channel Country Grouping");
        job.setJarByClass(ChannelCountryGrouping.class);
        job.setMapperClass(ChannelMapper.class);
        job.setCombinerClass(ChannelReducer.class);
        job.setReducerClass(ChannelReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

