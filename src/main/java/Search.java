import java.io.File;
import java.io.IOException;

import java.util.Scanner;
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


public class Search {
    public static class JoinMapperWC extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable k, Text value, Context context)
                throws IOException, InterruptedException {
            String[] a = value.toString().split("\t");
            context.write(new Text(a[0]), new Text(a[1]));
        }
    }
    public static class JoinMapperF extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable k, Text value, Context context)
                throws IOException, InterruptedException {
            String[] a = value.toString().split("\t");
            context.write(new Text(a[0]), new Text("TFIDF:"+a[1]));
        }
    }

    public static class JoinReducerT extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            DoubleWritable merge = new DoubleWritable(0);
            double name = 0;
            double dept = 0;
            String[] a = null;
            String[] b = null;
            String url = new File("").getAbsolutePath();
            File file = new File(url+"/queryTFIDF/part-r-00000");
            Scanner scanner = new Scanner(file);
            double qv = 0;
            while (scanner.hasNext()) {
                a = scanner.nextLine().split("\t");
                if (a[0].equals(key.toString())){
                    qv = Double.parseDouble(a[1]);
                    break;
                }
            }
            for(Text value : values) {
                if (!value.toString().contains("TFIDF:")) {
                    b = value.toString().split("@@@");
                    dept = Double.parseDouble(b[1]);
                    if (qv!=0.0) {
                        context.write(new Text(b[0]), new Text("" + dept * qv));
                    }
                }
            }

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(IDF.class);
//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(JoinReducerT.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String url = new File("").getAbsolutePath();
//        String input1 = url + "/outputT";
//        String input2 = url + "/outputWC";
        String outputUrl = url + "/querySearch";
        File outputFile=new File(outputUrl);
        if(outputFile.exists())
            FileUtils.deleteDirectory(outputFile);
        MultipleInputs.addInputPath(job, new Path("outputTFIDF"), TextInputFormat.class, JoinMapperWC.class);
        MultipleInputs.addInputPath(job, new Path("queryTFIDF"),TextInputFormat.class, JoinMapperF.class);
        FileOutputFormat.setOutputPath(job, new Path("querySearch"));
        job.waitForCompletion(true);
    }
}
