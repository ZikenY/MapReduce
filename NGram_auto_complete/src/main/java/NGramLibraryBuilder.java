
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NGramLibraryBuilder {
    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        int noGram;
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            noGram = conf.getInt("noGram", 5);
        }

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            String line = value.toString();
            
            line = line.trim().toLowerCase();
            line = line.replaceAll("[^a-z]", " ");  // replace all non-alphabet characters to spaces
            
            String[] words = line.split("\\s+");    // split by ' ', '\t'...ect
            
            if (words.length < 2) {                 // only use n>1_grams
                return;
            }
            
            // sliding window for N <= noGram
            // StringBuilder sb;
            for (int i = 0; i < words.length-1; i++) {
                StringBuilder sb = new StringBuilder();
                sb.append(words[i]);
                for (int offset = 1; i + offset < words.length && offset < noGram; offset++) {
                    sb.append(" ");
                    sb.append(words[i + offset]);

                    Text x_gram = new Text(sb.toString().trim());
                    context.write(x_gram, new IntWritable(1));
                }
            }
        }
    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // key = nGram
            // value = <1, 1, ...., 1>
            int count = 0;
            for (IntWritable value: values) {
                count += value.get();
            }

            // key: "see hot girl"
            // count: 100
            // output: "see hot girl\t100"
            context.write(key, new IntWritable(count));
        }
    }

}