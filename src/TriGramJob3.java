import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TriGramJob3 {
    public static class MyMapper extends Mapper<Text, Text, Text, Text> {
        private Text firstChar = new Text();
        private Text result = new Text();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            firstChar.set(key.toString().substring(0, 1));
            result.set(key.toString() + "\t" + 1);
            /* KEY: firstChar */
            /* VALUE: biGram, 1 */
            context.write(firstChar, result);
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /* KEY: firstChar */
            /* VALUE: biGram, 1 */
            List<String> biGrams = new ArrayList<>();
            long sum = 0;
            for (Text value : values) {
                sum += 1;
                biGrams.add(value.toString().substring(0, 2));
            }
            result.set("3" + "\t" + sum);
            for (String biGram : biGrams) {
                context.write(new Text(biGram), result);
            }
        }
    }
}