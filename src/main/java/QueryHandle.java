import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.Scanner;

public class QueryHandle {
    private static BufferedWriter writer;

    public static void main(String[] args) throws IOException {
        writer("Dima is Pidor x2");
        reader();
    }

    public static void writer(String input) throws IOException {
        String url = new File("").getAbsolutePath();
        String queries = url + "/queries";
        File file = new File(queries + "/queries.txt");

        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.write(input);
        writer.close();
    }

    public static String reader() throws IOException {
        String url = new File("").getAbsolutePath();
        String queries = url + "/queries";
        File file = new File(queries + "/queries.txt");

        Scanner scanner = new Scanner(file);
        return scanner.nextLine();
    }


}
