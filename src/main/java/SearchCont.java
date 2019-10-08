import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class SearchCont {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[]  valu = value.toString().split("\t");
            context.write(new Text(valu[0]),new DoubleWritable(Double.parseDouble(valu[1])));
        }

    }

    public static class IntSumReducer
            extends Reducer<Text, DoubleWritable, DoubleWritable, Text> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(result, key);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Counter.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);

        String url = new File("").getAbsolutePath();
        String inputUrl = url + "/querySearch";
        String outputUrl = url + "/Result";
        File outputFile=new File(outputUrl);
        if(outputFile.exists())
            FileUtils.deleteDirectory(outputFile);
        FileInputFormat.addInputPath(job, new Path(inputUrl));
        FileOutputFormat.setOutputPath(job, new Path("Result"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}