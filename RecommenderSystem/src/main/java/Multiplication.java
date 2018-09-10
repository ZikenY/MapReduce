import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Multiplication {
    // mapper 1
    public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input: movie_to \t movie_from=norm_relation
            // output: same as input (key为movie列ID)
            String[] movieTo_relation = value.toString().split("\t");
            context.write(new Text(movieTo_relation[0]), new Text(movieTo_relation[1]));
        }
    }

    // mapper 2
    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input:  "user,movie,rating" (Raw Data)
            // output: movie_id  ->  "user_id : rating"
            String[] line = value.toString().split(",");

            Text movie_id = new Text(line[1]);
            Text userID_rating = new Text(line[0] + ":" + line[2]);
            context.write(movie_id, userID_rating);
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // key: movie_to2;
            
            // value = <"movie_from1=norm_relation", movie_from3=norm_relation...     from CooccurrenceMapper
            //                  userA:rating, userB:rating...>                        from RatingMapper

            // relationMap: movie_fromID -> normalized_relation
            // ratingMap:   movie_(to)ID -> rating
            Map<String, Double> relationMap = new HashMap<String, Double>();
            Map<String, Double> ratingMap = new HashMap<String, Double>();

            // cross-multiply relationMap and ratingMap !!
            //    output each cell_result with the composite_key （to the later sum up MR）
            // output_key   = "user_id1:movie_from1", "user_id1:movie_from2",,,"user_id2:movie_from1",,,
            // output_value = norm_co[from1,to2]*rating[user1, from1], norm_co[from1,to2]*rating[user2, from1],,,,

            // distinguish the two type of data and make the 2 maps:
            for (Text value: values) {
                if (value.toString().contains("=")) {
                    String[] movie_relation = value.toString().split("=");
                    relationMap.put(movie_relation[0], Double.parseDouble(movie_relation[1]));
                } else {
                    String[] user_rating = value.toString().split(":");
                    ratingMap.put(user_rating[0], Double.parseDouble(user_rating[1]));
                }
            }

            // iterate the 2 map and compute the sub_expectation of each cell (by multiplication)
            for (Map.Entry<String, Double> entry: relationMap.entrySet()) {
                String movie_fromID = entry.getKey();
                double relation = entry.getValue();

                for (Map.Entry<String, Double> element: ratingMap.entrySet()) {
                    String user = element.getKey();
                    Text composite_key = new Text(user + ":" + movie_fromID);

                    double rating = element.getValue();
                    double sub_expectation = new DoubleWritable(relation*rating);

                    context.write(composite_key, sub_expectation);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(Multiplication.class);

        ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

        job.setMapperClass(CooccurrenceMapper.class);
        job.setMapperClass(RatingMapper.class);

        job.setReducerClass(MultiplicationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));
        
        job.waitForCompletion(true);
    }
}
