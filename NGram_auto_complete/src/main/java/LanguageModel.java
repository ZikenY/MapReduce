import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class LanguageModel {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        int threashold;
        // get the threashold parameter from the configuration
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            threashold = conf.getInt("threashold", 20);
        }

        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if((value == null) || (value.toString().trim()).length() == 0) {
                return;
            }
            //this is cool\t20
            String line = value.toString().trim();

            // output_key = "this is" (n-1 gram)
            // output_value = "cool = 20"
            
            String[] grams_count = line.split("\t");
            if (grams_count.length != 2) {
                return;
            }

            // grams_count[0] : n_gram
            // grams_count[1] : count
            
            String[] words = grams_count[0].split("\\s+");
            int count = Integer.valueOf(grams_count[1]);
            
            if(count < threashold) {
                return;        // not frequent enough
            }
            
            // outputKey:         从第一个单词粘连到倒数第二个单词
            // outputValue:        最后一个单词 = count 
            StringBuilder sb = new StringBuilder();
            for(int i = 0; i < words.length-1; i++) {
                sb.append(words[i]).append(" ");
            }
            String outputKey = sb.toString().trim();
            String outputValue = words[words.length - 1] + "=" + Integer.toString(count);
            
            // this is --> cool = 20
            if(!((outputKey == null) || (outputKey.length() <1))) {
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

        int k;

        // get the k parameter from the configuration
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            k = conf.getInt("k", 5);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // key:     "I like"
            // values:    "girl=10", "cat=20", "gal=6"...

            // sort on frequency
            // get top_k and write to mysql


            // reverseOrder_TreeMap (for sorting by frequency):
            // <20, cat>, <10, girl>, <6, gal>...
            TreeMap<Integer, List<String>> treemap = new TreeMap<Integer, List<String>>(Collections.reverseOrder());
            for (Text value: values) {
                // values里面存放的是last_word=frequency，把它们split开并存放在treemap中，
                String[] freq_word = value.toString().trim().split("=");
                String word = freq_word[0].trim();
                int count = Integer.parseInt(freq_word[1].trim());

                // 相同frequenct的放在同一个list下，属于同一个key
                // frequency大的自动放在前面
                if (treemap.containsKey(count)) {
                    treemap.get(count).add(word);
                } else {
                    List<String> list = new ArrayList<String>();
                    list.add(word);
                    treemap.put(count, list);
                }
            }

            // 取出treemap中多个word的前k个. 
            // <50, <girl, bird>> <60, <boy...>>
            Iterator<Integer> iter = treemap.keySet().iterator();
            for (int i=0; i<k && iter.hasNext();) {
                // 下一个frequency，注意不是k的下一个
                int frequency = iter.next();
                List<String> words = treemap.get(frequency);
                for (String word: words) {
                    //                                                "I like"   "girl"    10
                    DBOutputWritable db = new DBOutputWritable(key.toString(), word, frequency);
                    context.write(db, NullWritable.get());
                    // 注意每一个frequency可能有多个word，所以XOXOXO
                    i++;
                }
            }
        }
    }
}


/*
    "SELECT * FROM output WHERE starting_phrase LIKE "partial%" ORDER BY count DESC LIMIT 0, 10";

    select * from output
        where starting_phrase like 'input%'
        order by count
        limit 10;
*/