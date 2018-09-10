import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Normalize {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //movieA:movieB \t relation
            String[] movie_relation = value.toString().trim().split("\t");
            String[] movies = movie_relation[0].split(":");

            context.write(new Text(movies[0]),                              // key = movie_from
                          new Text(movies[1] + ":" + movie_relation[1])     // value = movie_to : relation
                          );
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        // reduce method
        @Override   // remember key (input_key) = movie_fromID
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // input_key = movieA, input_value=<movieB:relation, movieC:relation...>
            // sum up relations to get denominator
            // iterate relations and divide each relation by denominator (relation/denominator) to get normalized_relation
            // output_key = movie_To, output_value = "movie_from = normalized_relation"


            // to_ID -> relation
            Map<String, Integer> map = new HashMap<String, Integer>();
            int denominator = 0;
            while (values.iterator().hasNext()) {
                String[] movie_relation = values.iterator().next().toString().split(":");
                int relation = Integer.parseInt(movie_relation[1]);
                denominator += relation;
                map.put(movie_relation[0], relation);
            }

            // normalizing
            for(Map.Entry<String, Integer> entry: map.entrySet()) {
                double norm_relation = (double)entry.getValue() / denominator;

                String outputKey = entry.getKey();                         //  movie_toID
                String outputValue = key.toString() + "=" + norm_relation; // "movie_fromID : norm_relation"
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(Normalize.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
