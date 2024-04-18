import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopNWordCountPairs {

    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().toLowerCase().split("\\W+");

            String lastWord = "";
            for (String word : words) {
                if (!lastWord.isEmpty() && word.matches("[a-z]+")) {
                    context.write(new Text(lastWord + ":" + word), one);
                }

                if( word.matches("[0-9]+")) {
					lastWord = word;
				}
                
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable wordcount = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int word_cnt = 0;

            for (IntWritable value : values)
                word_cnt += value.get();

            wordcount.set(word_cnt);

            context.write(key, wordcount);
        }
    }

    public static class TopNMapper extends Mapper<Object, Text, Text, IntWritable> {
        private int n;
        private TreeMap<Integer, String> word_list;

        public void setup(Context context) {
            n = Integer.parseInt(context.getConfiguration().get("N"));
            word_list = new TreeMap<>();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");

            word_list.put(Integer.valueOf(line[1]), line[0]);

            if (word_list.size() > n)
                word_list.remove(word_list.firstKey());
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> entry : word_list.entrySet()) {
                context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
            }
        }
    }

    public static class TopNReducer extends Reducer<Text, IntWritable, IntWritable, Text> {
        private int n;
        private TreeMap<Integer, String> word_list;

        public void setup(Context context) {
            n = Integer.parseInt(context.getConfiguration().get("N"));
            word_list = new TreeMap<>();
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int wordcount = 0;

            for (IntWritable value : values)
                wordcount = value.get();

            word_list.put(wordcount, key.toString());

            if (word_list.size() > n)
                word_list.remove(word_list.firstKey());
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> entry : word_list.descendingMap().entrySet()) {
                context.write(new IntWritable(entry.getKey()), new Text(entry.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] pathArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        conf.set("N", "100");

        if (pathArgs.length < 2) {
            System.err.println("MR Project Usage: TopNWordCountPairs <input-path> [...] <output-path>");
            System.exit(2);
        }

        Path wordcount_dir = new Path("wordcount");
        Path output_dir = new Path(pathArgs[pathArgs.length - 1]);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(wordcount_dir))
            fs.delete(wordcount_dir, true);
        if (fs.exists(output_dir))
            fs.delete(output_dir, true);

        Job wc_job = Job.getInstance(conf, "WordCountPairs");
        wc_job.setJarByClass(TopNWordCountPairs.class);
        wc_job.setMapperClass(WordCountMapper.class);
        wc_job.setReducerClass(WordCountReducer.class);
        wc_job.setMapOutputKeyClass(Text.class);
        wc_job.setMapOutputValueClass(IntWritable.class);
        wc_job.setOutputKeyClass(Text.class);
        wc_job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < pathArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(wc_job, new Path(pathArgs[i]));
        }
        FileOutputFormat.setOutputPath(wc_job, wordcount_dir);
        wc_job.waitForCompletion(true);

        Job topn_job = Job.getInstance(conf, "TopNWordCountPairs");
        topn_job.setJarByClass(TopNWordCountPairs.class);
        topn_job.setMapperClass(TopNMapper.class);
        topn_job.setReducerClass(TopNReducer.class);
        topn_job.setMapOutputKeyClass(Text.class);
        topn_job.setMapOutputValueClass(IntWritable.class);
        topn_job.setOutputKeyClass(IntWritable.class);
        topn_job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(topn_job, wordcount_dir);
        FileOutputFormat.setOutputPath(topn_job, output_dir);
        topn_job.waitForCompletion(true);
    }
}
