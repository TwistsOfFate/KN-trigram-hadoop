import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class TriGramJob8 {
    public static class MyMapper extends Mapper<Text, Text, Text, Text> {
        private Text result = new Text();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String ngram = key.toString();
            if (ngram.length() == 2) {
                context.write(key, value);
            } else if (ngram.length() == 3) {
                String prefix2 = ngram.substring(0, 2);
                /* Append the third character to value */
                result.set(value.toString() + "\t" + ngram.charAt(2));
                context.write(new Text(prefix2), result);
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, long[]> valMap = new HashMap<>();
            HashMap<String, Double> doubleMap = new HashMap<>();
            for (Text value : values) {
                String[] tokens = value.toString().split("\\s+");
                int valType = Integer.parseInt(tokens[0]);
                if (valType == 7) {
                    doubleMap.putIfAbsent(key.toString(), Double.valueOf(tokens[1]));
                } else {
                    String triGram = key.toString();
                    if (valType == 2) {
                        triGram += tokens[3];
                    } else {
                        triGram += tokens[2];
                    }
                    long[] tmpArray = new long[10];
                    /*
                        tmpArray:
                        [2] c(w1, w2, w3) from job2
                        [3] c(w1, w2, *) from job2
                        [6] d(w1, w2, *) from job6
                    */
                    if (valMap.containsKey(triGram)) {
                        tmpArray = valMap.get(triGram);
                    }
                    tmpArray[valType] = Long.parseLong(tokens[1]);
                    if (valType == 2) {
                        tmpArray[3] = Long.parseLong(tokens[2]);
                    }
                    valMap.put(triGram, tmpArray);
                }
            }
            for (HashMap.Entry<String, long[]> valEntry : valMap.entrySet()) {
                String prefix2 = valEntry.getKey().substring(0, 2);
                long[] value = valEntry.getValue();
                double prob = ((double)value[2] - TriGram.delta) / value[3];
                prob += TriGram.delta * value[6] / value[3] * doubleMap.get(prefix2);
                result.set("8" + "\t" + prob);
                context.write(new Text(valEntry.getKey()), result);
            }
        }
    }
}