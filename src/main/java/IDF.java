import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class IDF {
    public static class JoinMapperWC extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable k, Text value, Context context)
                throws IOException, InterruptedException {
            String[] a = value.toString().split("\t");
            Text ret = null;
            Text val = null;
            if (a[0].equals("1")){
                ret = new Text("1");
                val = new Text("00000000000000all@@@"+a[1]);
            }
            else{
                ret = new Text("1");
                val = new Text(a[1]+"@@@"+a[0]);
            }
            context.write(ret, val);
        }
    }

    public static class JoinReducerT extends Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            DoubleWritable merge = new DoubleWritable(0);
            double name = 0;
            double dept = 0;
            String[] a = null;
            double v = 0;
            for(Text value : values) {
                if (value.toString().startsWith("00000000000000all@@@")) {
                    a = value.toString().split("@@@");
                    name = Double.parseDouble(a[1]);
                } else {
                    a = value.toString().split("@@@");
                    dept = Double.parseDouble(a[0]);
                    if ((name!=0) && (dept!=0)) {
                        merge.set(Math.log(name / dept));
                    }
                    context.write(new Text(a[1]), merge);
                }
            }

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(IDF.class);
        job.setMapperClass(JoinMapperWC.class);
//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(JoinReducerT.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        String url = new File("").getAbsolutePath();
//        String input1 = url + "/outputT";
//        String input2 = url + "/outputWC";
        String outputUrl = url + "/outputIDF";
        File outputFile=new File(outputUrl);
        if(outputFile.exists())
            FileUtils.deleteDirectory(outputFile);
        MultipleInputs.addInputPath(job, new Path("outputC"), TextInputFormat.class);

        MultipleInputs.addInputPath(job, new Path("outputTF"),TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("outputIDF"));
        job.waitForCompletion(true);
    }
}
