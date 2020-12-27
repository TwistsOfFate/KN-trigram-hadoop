import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TriGramJob1 {
    public static class MyMapper extends Mapper<Object, Text, Text, MapWritable> {
        private final static LongWritable one = new LongWritable(1);
        private Text firstChar = new Text();
        private MapWritable result = new MapWritable();

        private boolean isChineseChar(char ch) {
            return ch >= 0x4E00 && ch <= 0x9FA5;
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            int strLength = str.length();

            for (int i = 0; i < strLength; ++i) {
                if (i >= 1) {
                    char ch0 = str.charAt(i-1);
                    char ch1 = str.charAt(i);
                    if (isChineseChar(ch0) && isChineseChar(ch1)) {
                        firstChar.set("" + ch0);
                        result.clear();
                        result.put(new Text(String.valueOf(ch1)), one);
                        context.write(firstChar, result);
                    }
                }
            }
        }
    }

    public static class MyReducer extends Reducer<Text, MapWritable, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<MapWritable> maps, Context context) throws IOException, InterruptedException {
            /* sumMap<String biGram, Long count> */
            Map<String, Long> sumMap = new HashMap<>();
            long sum = 0;
            String firstChar = key.toString();
            for (MapWritable map : maps) {
                /* map<String secondChar, long count> */
                for (Map.Entry<Writable, Writable> kv : map.entrySet()) {
                    String secondChar = kv.getKey().toString();
                    long count = ((LongWritable) kv.getValue()).get();
                    String biGram = firstChar + secondChar;
                    sum += count;

                    /* Calculate normal (biGram, count) */
                    if (sumMap.containsKey(biGram)) {
                        Long oldCount = sumMap.get(biGram);
                        sumMap.put(biGram, oldCount + count);
                    } else {
                        sumMap.put(biGram, count);
                    }
                }
            }
            for (String biGram : sumMap.keySet()) {
                long count = sumMap.get(biGram);
                result.set("1" + '\t' + count + '\t' + sum);
                context.write(new Text(biGram), result);
            }
        }
    }
}