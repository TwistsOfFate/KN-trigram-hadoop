import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TriGram {
    public static final double delta = 0.75;

    public static void main(String[] args) throws Exception {
        Path job0Path = new Path("job0/");
        Path job1Path = new Path("job1/");
        Path job2Path = new Path("job2/");
        Path job3Path = new Path("job3/");
        Path job4Path = new Path("job4/");
        Path job5Path = new Path("job5/");
        Path job6Path = new Path("job6/");
        Path job7Path = new Path("job7/");
        Path job8Path = new Path("job8/");
        Path job9Path = new Path("job9/");

        Configuration conf = new Configuration();

        conf.set("mapreduce.map.memory.mb", "2048");
        conf.set("mapreduce.reduce.memory.mb", "2048");
        conf.set("mapreduce.map.maxattempts", "12");
        conf.set("mapreduce.reduce.maxattempts", "12");
        conf.set("mapreduce.task.timeout", "6000000");
        conf.set("mapreduce.input.fileinputformat.split.maxsize", "33554432");

        if (args.length > 1 && args[1].equalsIgnoreCase("raw")) {
            Job job0 = Job.getInstance(conf, "TriGram: Raw");
            job0.setJarByClass(TriGram.class);
            job0.setNumReduceTasks(50);
            job0.setMapperClass(Raw.RawMapper.class);
            job0.setMapOutputKeyClass(Text.class);
            job0.setMapOutputValueClass(MapWritable.class);
            job0.setReducerClass(Raw.RawReducer.class);
            job0.setOutputKeyClass(Text.class);
            job0.setOutputValueClass(Text.class);

            job0Path.getFileSystem(conf).delete(job0Path, true);
            FileInputFormat.addInputPath(job0, new Path("/corpus/news_sohusite.xml"));
            if (args.length > 0 && args[0].equals("news")) {
                FileInputFormat.addInputPath(job0, new Path("mycorpus/news2016zh_train.json"));
                FileInputFormat.addInputPath(job0, new Path("mycorpus/news2016zh_valid.json"));
            } else if (args.length > 0 && args[0].equals("full")) {
                FileInputFormat.addInputPath(job0, new Path("mycorpus/"));
            }
            FileOutputFormat.setOutputPath(job0, job0Path);
            if (!job0.waitForCompletion(true)) System.exit(1);
        } else {
            /* Job1 begins */
            Job job1 = Job.getInstance(conf, "TriGram: 1");
            job1.setJarByClass(TriGram.class);
            job1.setNumReduceTasks(40);
            job1.setMapperClass(TriGramJob1.MyMapper.class);
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(MapWritable.class);
            job1.setReducerClass(TriGramJob1.MyReducer.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);

            job1Path.getFileSystem(conf).delete(job1Path, true);
            FileInputFormat.addInputPath(job1, new Path("/corpus/news_sohusite.xml"));
            FileOutputFormat.setOutputPath(job1, job1Path);
            if (!job1.waitForCompletion(true)) System.exit(1);

            /* Job2 begins */
            Job job2 = Job.getInstance(conf, "TriGram: 2");
            job2.setJarByClass(TriGram.class);
            job2.setNumReduceTasks(40);
            job2.setMapperClass(TriGramJob2.MyMapper.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(MapWritable.class);
            job2.setReducerClass(TriGramJob2.MyReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            job2Path.getFileSystem(conf).delete(job2Path, true);
            FileInputFormat.addInputPath(job2, new Path("/corpus/news_sohusite.xml"));
            FileOutputFormat.setOutputPath(job2, job2Path);
            if (!job2.waitForCompletion(true)) System.exit(1);

            /* Job3 begins */
            Job job3 = Job.getInstance(conf, "TriGram: 3");
            job3.setJarByClass(TriGram.class);
            job3.setMapperClass(TriGramJob3.MyMapper.class);
            job3.setMapOutputKeyClass(Text.class);
            job3.setMapOutputValueClass(Text.class);
            job3.setReducerClass(TriGramJob3.MyReducer.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(Text.class);

            job3Path.getFileSystem(conf).delete(job3Path, true);
            job3.setInputFormatClass(KeyValueTextInputFormat.class);
            KeyValueTextInputFormat.addInputPath(job3, job1Path);
            FileOutputFormat.setOutputPath(job3, job3Path);
            if (!job3.waitForCompletion(true)) System.exit(1);

            /* Job4 begins */
            Job job4 = Job.getInstance(conf, "TriGram: 4");
            job4.setJarByClass(TriGram.class);
            job4.setMapperClass(TriGramJob4.MyMapper.class);
            job4.setMapOutputKeyClass(Text.class);
            job4.setMapOutputValueClass(Text.class);
            job4.setReducerClass(TriGramJob4.MyReducer.class);
            job4.setOutputKeyClass(Text.class);
            job4.setOutputValueClass(Text.class);

            job4Path.getFileSystem(conf).delete(job4Path, true);
            job4.setInputFormatClass(KeyValueTextInputFormat.class);
            KeyValueTextInputFormat.addInputPath(job4, job1Path);
            FileOutputFormat.setOutputPath(job4, job4Path);
            if (!job4.waitForCompletion(true)) System.exit(1);

            /* Job5 begins */
            Job job5 = Job.getInstance(conf, "TriGram: 5");
            job5.setJarByClass(TriGram.class);
            job5.setMapperClass(TriGramJob5.MyMapper.class);
            job5.setMapOutputKeyClass(Text.class);
            job5.setMapOutputValueClass(Text.class);
            job5.setReducerClass(TriGramJob5.MyReducer.class);
            job5.setOutputKeyClass(Text.class);
            job5.setOutputValueClass(Text.class);

            job5Path.getFileSystem(conf).delete(job5Path, true);
            job5.setInputFormatClass(KeyValueTextInputFormat.class);
            KeyValueTextInputFormat.addInputPath(job5, job1Path);
            FileOutputFormat.setOutputPath(job5, job5Path);
            if (!job5.waitForCompletion(true)) System.exit(1);

            /* Job6 begins */
            Job job6 = Job.getInstance(conf, "TriGram: 6");
            job6.setJarByClass(TriGram.class);
            job6.setMapperClass(TriGramJob6.MyMapper.class);
            job6.setMapOutputKeyClass(Text.class);
            job6.setMapOutputValueClass(Text.class);
            job6.setReducerClass(TriGramJob6.MyReducer.class);
            job6.setOutputKeyClass(Text.class);
            job6.setOutputValueClass(Text.class);

            job6Path.getFileSystem(conf).delete(job6Path, true);
            job6.setInputFormatClass(KeyValueTextInputFormat.class);
            KeyValueTextInputFormat.addInputPath(job6, job2Path);
            FileOutputFormat.setOutputPath(job6, job6Path);
            if (!job6.waitForCompletion(true)) System.exit(1);

            /* Job7 begins */
            Job job7 = Job.getInstance(conf, "TriGram: 7");
            job7.setJarByClass(TriGram.class);
            job7.setNumReduceTasks(20);
            job7.setMapperClass(TriGramJob7.MyMapper.class);
            job7.setMapOutputKeyClass(Text.class);
            job7.setMapOutputValueClass(Text.class);
            job7.setReducerClass(TriGramJob7.MyReducer.class);
            job7.setOutputKeyClass(Text.class);
            job7.setOutputValueClass(Text.class);

            job7Path.getFileSystem(conf).delete(job7Path, true);
            job7.setInputFormatClass(KeyValueTextInputFormat.class);
            KeyValueTextInputFormat.addInputPath(job7, job1Path);
            KeyValueTextInputFormat.addInputPath(job7, job3Path);
            KeyValueTextInputFormat.addInputPath(job7, job4Path);
            KeyValueTextInputFormat.addInputPath(job7, job5Path);
            FileOutputFormat.setOutputPath(job7, job7Path);
            if (!job7.waitForCompletion(true)) System.exit(1);

            /* Job8 begins */
            Job job8 = Job.getInstance(conf, "TriGram: 8");
            job8.setJarByClass(TriGram.class);
            job8.setNumReduceTasks(20);
            job8.setMapperClass(TriGramJob8.MyMapper.class);
            job8.setMapOutputKeyClass(Text.class);
            job8.setMapOutputValueClass(Text.class);
            job8.setReducerClass(TriGramJob8.MyReducer.class);
            job8.setOutputKeyClass(Text.class);
            job8.setOutputValueClass(Text.class);

            job8Path.getFileSystem(conf).delete(job8Path, true);
            job8.setInputFormatClass(KeyValueTextInputFormat.class);
            KeyValueTextInputFormat.addInputPath(job8, job2Path);
            KeyValueTextInputFormat.addInputPath(job8, job6Path);
            KeyValueTextInputFormat.addInputPath(job8, job7Path);
            FileOutputFormat.setOutputPath(job8, job8Path);
            if (!job8.waitForCompletion(true)) System.exit(1);

            /* Job9 begins */
            Job job9 = Job.getInstance(conf, "TriGram: 9");
            job9.setJarByClass(TriGram.class);
            job9.setMapperClass(TriGramJob9.MyMapper.class);
            job9.setMapOutputKeyClass(Text.class);
            job9.setMapOutputValueClass(Text.class);
            job9.setReducerClass(TriGramJob9.MyReducer.class);
            job9.setOutputKeyClass(Text.class);
            job9.setOutputValueClass(Text.class);

            job9Path.getFileSystem(conf).delete(job9Path, true);
            FileInputFormat.addInputPath(job9, new Path("/corpus/news_sohusite.xml"));
            FileOutputFormat.setOutputPath(job9, job9Path);
            if (!job9.waitForCompletion(true)) System.exit(1);
        }

        System.exit(0);
    }
}