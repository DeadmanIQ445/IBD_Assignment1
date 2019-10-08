import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SearchEngine {

    public static void main(String[] args) throws IOException {
        String searchWord = "zab";
        char[] searchWordToChar = searchWord.toCharArray();

        String url = new File("").getAbsolutePath();
        String searchBase = url + "/outputT/part-r-00000";
        File file = new File(searchBase);

        FileReader fr = new FileReader(file);
        BufferedReader reader = new BufferedReader(fr);
        String line = reader.readLine();

        Boolean found;
        while (line != null) {
            line = reader.readLine();

            if (line != null) {
                char[] lineToChar = line.toCharArray();

                found = true;
                for (int i = 0; i < searchWord.length(); i++) {
                    if (searchWordToChar[i] != lineToChar[i])
                        found = false;
                }
                if (lineToChar[searchWord.length()] != ' ' && lineToChar[searchWord.length()] != '\t')
                    found = false;

                if (found)
                    System.out.println(line);
            }
        }
    }
}