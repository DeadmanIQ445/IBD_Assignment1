import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String valueStr = value.toString();
            JSONObject jsonObject = null;
            try {
                jsonObject = new JSONObject(valueStr);
                Text words = new Text(jsonObject.getString("text"));


                StringTokenizer itr = new StringTokenizer(words.toString(),"\'\n.,!?:(){}[]<>/;“”‘\"#$ -+&%*");
                while (itr.hasMoreTokens()) {
                    String  word = itr.nextToken().toLowerCase();
                    if (word.equals("")) continue;
                    context.write(new Text(word),one);
                }

            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        String url = new File("").getAbsolutePath();
        String inputUrl = url + "/target/classes";
        String outputUrl = url + "/outputWC";
        File outputFile=new File(outputUrl);
        if(outputFile.exists())
            FileUtils.deleteDirectory(outputFile);
        FileInputFormat.addInputPath(job, new Path(inputUrl));
        FileOutputFormat.setOutputPath(job, new Path("outputWC"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}