import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TriGramJob9 {
    public static class MyMapper extends Mapper<Object, Text, Text, Text> {
        private final Text uniKey = new Text("X");
        private Text result = new Text();

        private boolean isChineseChar(char ch) {
            return ch >= 0x4E00 && ch <= 0x9FA5;
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            int strLength = str.length();
            HashMap<Character, Long> countMap = new HashMap<>();

            for (int i = 0; i < strLength; ++i) {
                if (i >= 1) {
                    char ch = str.charAt(i);
                    if (isChineseChar(ch)) {
                        if (!countMap.containsKey(ch)) {
                            countMap.put(ch, 1L);
                        } else {
                            Long oldCount = countMap.get(ch);
                            countMap.put(ch, oldCount + 1L);
                        }
                    }
                }
            }

            for (HashMap.Entry<Character, Long> entry : countMap.entrySet()) {
                result.set(entry.getKey().toString() + "\t" + entry.getValue());
                context.write(uniKey, result);
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Long> sumMap = new HashMap<>();
            long bigSum = 0L;
            for (Text value : values) {
                String[] tokens = value.toString().split("\\s+");
                long count = Long.parseLong(tokens[1]);
                if (!sumMap.containsKey(tokens[0])) {
                    sumMap.put(tokens[0], count);
                } else {
                    Long oldCount = sumMap.get(tokens[0]);
                    sumMap.put(tokens[0], oldCount + count);
                }
                bigSum += count;
            }

            for (HashMap.Entry<String, Long> entry : sumMap.entrySet()) {
                double p = (double)entry.getValue() / bigSum;
                result.set("9" + "\t" + p);
                context.write(new Text(entry.getKey()), result);
            }
        }
    }
}