import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.management.Query;
import java.io.*;
import java.util.Scanner;

public class ResultHandle {
    private static BufferedWriter writer;

    public static void writer(String input) throws IOException {
        String url = new File("").getAbsolutePath();
        String queries = url + "/query";
        File file = new File(queries);
        file.mkdirs();
        file = new File(queries + "/query.txt");
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.write("{\"text\":\""+input + "\"}");
        writer.close();
    }


    public static String reader() throws IOException {
        String url = new File("").getAbsolutePath();
        String queries = url + "/result";
        File file = new File(queries + "/part-r-00000");

        Scanner scanner = new Scanner(file);
        return scanner.nextLine();
    }

    public static void main(String[] arg) throws Exception {
        Scanner scanner = new Scanner(System.in);
        while (true){
            writer(scanner.nextLine());
            WordCount.main(arg);
            Counter.main(arg);
            CounterByFile.main(arg);
            Tokenizer.main(arg);
            TF.main(arg);
            IDF.main(arg);
            QueryWC.main(arg);
            TFIDF.main(arg);
            QueryTFIDF.main(arg);
            Search.main(arg);
            SearchCont.main(arg);
            AAAAAA.main(arg);
        }
    }


}
