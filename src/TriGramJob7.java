import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class TriGramJob7 {
    public static class MyMapper extends Mapper<Text, Text, Text, Text> {

    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, long[]> valMap = new HashMap<>();
            String biGram = key.toString();
            for (Text value : values) {
                String[] tokens = value.toString().split("\\s+");
                int valType = Integer.parseInt(tokens[0]);
                long[] tmpArray = new long[10];
                /*
                    tmpArray:
                    [1] c(w1, w2) from job1
                    [2] c(w1, *) from job2
                    [3] d(w1, *) from job3
                    [4] d(*, w2) from job4
                    [5] d(*, *) from job5
                */
                if (valMap.containsKey(biGram)) {
                    tmpArray = valMap.get(biGram);
                }
                tmpArray[valType] = Long.parseLong(tokens[1]);
                if (valType == 1) {
                    tmpArray[2] = Long.parseLong(tokens[2]);
                }
                valMap.put(biGram, tmpArray);
            }
            for (HashMap.Entry<String, long[]> valEntry : valMap.entrySet()) {
                long[] value = valEntry.getValue();
                double prob = ((double)value[1] - TriGram.delta) / value[2];
                prob += TriGram.delta * value[3] / value[2] * value[4] / value[5];
                result.set("7" + "\t" + prob);
                context.write(key, result);
            }
        }
    }
}