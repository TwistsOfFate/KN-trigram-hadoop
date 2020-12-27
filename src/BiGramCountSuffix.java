import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BiGramCountSuffix {
    public static class BGCSMapper extends Mapper<Text, Text, Text, Text> {
        private Text result = new Text();

        /* INPUT key: biGram, value: {count sum1} */
        /* OUTPUT key: secondChar, value: {biGram count sum1} */
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String secondChar = key.toString().substring(1, 2);
            result.set(key.toString() + "\t" + value.toString());
            context.write(new Text(secondChar), result);
        }
    }

    public static class BGCSReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sum2 = 0;
            for (Text value : values) {
                /* value: {biGram count sum1} */
                String[] tokens = value.toString().split("\\s+");
                sum2 += Long.parseLong(tokens[2]);
            }
            for (Text value : values) {
                /* value: {biGram count sum1} */
                String[] tokens = value.toString().split("\\s+");
                result.set(tokens[1] + "\t" + tokens[2] + "\t" + sum2);
                context.write(new Text(tokens[0]), result);
            }
        }
    }
}
