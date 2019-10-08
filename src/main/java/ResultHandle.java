import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.Scanner;

public class ResultHandle {
    private static BufferedWriter writer;

    public static void writer(String input) throws IOException {
        String url = new File("").getAbsolutePath();
        String queries = url + "/result";
        File file = new File(queries + "/part-r-00000");

        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.write(input);
        writer.close();
    }

    public static String reader() throws IOException {
        String url = new File("").getAbsolutePath();
        String queries = url + "/result";
        File file = new File(queries + "/part-r-00000");

        Scanner scanner = new Scanner(file);
        return scanner.nextLine();
    }


}
