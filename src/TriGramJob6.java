import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TriGramJob6 {
    public static class MyMapper extends Mapper<Text, Text, Text, Text> {
        private Text firstTwoChars = new Text();
        private Text result = new Text();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            firstTwoChars.set(key.toString().substring(0, 2));
            result.set(key.toString() + "\t" + 1);
            /* KEY: firstTwoChars */
            /* VALUE: triGram, 1 */
            context.write(firstTwoChars, result);
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /* KEY: firstTwoChars */
            /* VALUE: triGram, 1 */
            List<String> triGrams = new ArrayList<>();
            long sum = 0;
            for (Text value : values) {
                sum += 1;
                triGrams.add(value.toString().substring(0, 3));
            }
            for (String triGram : triGrams) {
                result.set("6" + "\t" + sum);
                context.write(new Text(triGram), result);
            }
        }
    }
}