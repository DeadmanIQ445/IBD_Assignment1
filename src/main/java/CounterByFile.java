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

public class CounterByFile {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String valueStr = value.toString();
            JSONObject jsonObject = null;
            try {
                jsonObject = new JSONObject(valueStr);
                Text words = new Text(jsonObject.getString("text"));


                StringTokenizer itr = new StringTokenizer(words.toString(), "\'\n.,!?:(){}[]<>/;“”‘\"#$ -+&%*");
                while (itr.hasMoreTokens()) {
                    String word = itr.nextToken().toLowerCase();
                    Text wordText = new Text(word);
                    Text count = new Text(one+"@@@"+jsonObject.getString("id"));
                    context.write(wordText, count);
                }

            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            String[] a =  null;
            for (Text val : values) {
                a =  val.toString().split("@@@");
                sum += Integer.parseInt(a[0]);
            }
            result.set(sum);
            Text value2 = new Text(a[1] + "@@@" + result);
            context.write(key, value2);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Counter.class);
        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String url = new File("").getAbsolutePath();
        String inputUrl = url + "/src/main/resources";
        String outputUrl = url + "/outputCBF";
        File outputFile = new File(outputUrl);
        if (outputFile.exists())
            FileUtils.deleteDirectory(outputFile);
        FileInputFormat.addInputPath(job, new Path(inputUrl));
        FileOutputFormat.setOutputPath(job, new Path("outputCBF"));
        job.waitForCompletion(true);
    }
}