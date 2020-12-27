import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Raw {
    public static class RawMapper extends Mapper<Object, Text, Text, MapWritable> {
        private MapWritable result = new MapWritable();
//        private HashMap<Text, HashMap<Text, LongWritable>> map1 = new HashMap<>();
//        private HashMap<Text, LongWritable> map2 = new HashMap<>();

        private final static LongWritable one = new LongWritable(1);
        private Text firstTwoChars = new Text();
        private Text thirdChar = new Text();

        private boolean isChineseChar(char ch) {
            return ch >= 0x4E00 && ch <= 0x9FA5;
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            int strLength = str.length();

            for (int i = 0; i < strLength; ++i) {
                if (i >= 2) {
                    char ch0 = str.charAt(i-2);
                    char ch1 = str.charAt(i-1);
                    char ch2 = str.charAt(i);

                    if (isChineseChar(ch0) && isChineseChar(ch1) && isChineseChar(ch2)) {
                        firstTwoChars.set("" + ch0 + ch1);
                        thirdChar.set("" + ch2);

//                        if (map1.containsKey(firstTwoChars)) {
//                            map2 = map1.get(firstTwoChars);
//                            if (map2.containsKey(thirdChar)) {
//                                LongWritable oldValue = (LongWritable)map2.get(thirdChar);
//                                LongWritable oldValuePlusOne = new LongWritable(oldValue.get() + 1);
//                                map2.put(thirdChar, oldValuePlusOne);
//                            } else {
//                                map2.put(thirdChar, one);
//                            }
//                        } else {
//                            map2 = new HashMap<>();
//                            map2.put(thirdChar, one);
//                        }
//                        map1.put(thirdChar, map2);

                        result.clear();
                        result.put(thirdChar, one);
                        context.write(firstTwoChars, result);
                    }
                }
            }

//            for (Map.Entry<Text, HashMap<Text, LongWritable>> kv : map1.entrySet()) {
//                result.clear();
//                result.putAll(kv.getValue());
//                context.write(kv.getKey(), result);
//            }
        }
    }

    public static class RawReducer extends Reducer<Text, MapWritable, Text, Text> {
        public void reduce(Text key, Iterable<MapWritable> maps, Context context) throws IOException, InterruptedException {
            Map<String, Long> sumMap = new HashMap<>();
            long sum = 0;
            for (MapWritable map : maps) {
                for (Map.Entry<Writable, Writable> kv : map.entrySet()) {
                    String triGram = key.toString() + kv.getKey().toString();
                    long triGramCount = ((LongWritable)kv.getValue()).get();
                    sum += triGramCount;
                    if (sumMap.containsKey(triGram)) {
                        Long oldCount = sumMap.get(triGram);
                        sumMap.put(triGram, oldCount + triGramCount);
                    } else {
                        sumMap.put(triGram, triGramCount);
                    }
                }
            }
            for (String triGram : sumMap.keySet()) {
                long count = (long)sumMap.get(triGram);
                double quotient = (double)count / sum;
                String result = triGram.substring(2,3) + '\t' + count + '\t' + sum + '\t' + quotient;
                context.write(new Text(triGram.substring(0, 2)), new Text(result));
            }
        }
    }
}
