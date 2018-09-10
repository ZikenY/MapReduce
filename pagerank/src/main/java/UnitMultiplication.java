import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/*
    use 2 mappers to read transition_matrix and PR_matrix(a col vector)，
    mapper1 output_id is the from_id (col#)，corresponding to mapper2's row#，

    reducer input: from_id -> [to_id:probability, to_id:probability,..., pr0]
            output: [<to_id, prob * pr0>, <to_id, prob * pr0>, ....]
*/
public class UnitMultiplication {
    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // transition.txt: from_id \t to_id,to_id,...,to_id
            // process: split to_id list, calculate probability
            // output_key = from_id
            // output_val = "to_id=probility"

            String[] form_to = value.toString().trim().split("\t");

            if (from_to.length == 1) {
                // no output ref. dead end
                return;
            }

            // from_to[0]: output_key
            String[] tos = from_to[1].split(",");
            String output_key = from_to[0];
            for (String to : tos) {
                String output_val = to + "=" + (double)1 / tos.length;
                context.write(new Text(output_key), new Text(output_val));
            }
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // pr.txt: from_id \t pr_value
            // output_key = from_id
            // output_val = pr0_value
            String[] id_pr = value.toString().trim().split("\t");
            context.write(new Text(id_pr[0]), new Text(id_pr[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // input_key = from_id 
            // value = "to_id=prob" or "pr0_value"
            // progress: use a list<string> to store all "to_id=prob"s. 
            //           use pr0 to store "pr0_value"
            //           context.write(to_id, prob * pr0)
            double pr0 = 0;
            List<String> cells = new ArrayList<String>();
            for (Text value : values) {
                if (value.toString().contains("=")) {
                    cells.add(value.toString);
                } else {
                    pr0 = Double.parseDouble(value.toString());
                }
            }

            for (String cell : cells) {
                // cell: "to_id=0.33333"
                String[] id_prob = cell.split("=");
                String to_id = id_prob[0];
                double prob = Double.parseDouble(id_prob[1]);
                context.write(new Text(to_id), new Text(String.valueOf(prob * pr0)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        //how chain two mapper classes?
        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
