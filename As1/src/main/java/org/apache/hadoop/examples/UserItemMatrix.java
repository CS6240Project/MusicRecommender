package org.apache.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class UserItemMatrix {

  public static class RatingMapper extends Mapper<Object, Text, Text, Text> {

    private Text userId = new Text();
    private Text itemAndScore = new Text();
    private boolean isUserIdLine = false;  // 标记当前是否为用户ID行

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString().trim();

      // 如果当前行为用户ID行
      if (line.contains("|")) {
        // 将UserId提取出来
        String[] parts = line.split("\\|");
        if (parts.length != 2) return;  // 跳过格式错误的行
        userId.set(parts[0]);  // 设置UserId作为键
        isUserIdLine = true;   // 标记为用户ID行
      } else if (isUserIdLine) {
        // 如果不是用户ID行，则是评分记录行
        String[] fields = line.split("\t");
        if (fields.length < 2) return;  // 跳过格式错误的行

        // 提取ItemId和Score
        String itemId = fields[0];
        String score = fields[1];
        itemAndScore.set(itemId + ":" + score);  // 拼接成ItemId:Score格式

        // 输出 (UserId, ItemId:Score)
        context.write(userId, itemAndScore);
      }
    }
  }

  public static class MatrixReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      StringBuilder ratings = new StringBuilder();
      for (Text val : values) {
        if (ratings.length() > 0) {
          ratings.append(", ");
        }
        ratings.append(val.toString());  // 追加ItemId:Score
      }
      result.set(ratings.toString());
      context.write(key, result);  // 输出 (UserId, ItemId1:Score1, ItemId2:Score2, ...)
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: UserItemMatrix <in> <out>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "User-Item Rating Matrix");
    job.setJarByClass(UserItemMatrix.class);
    job.setMapperClass(RatingMapper.class);
    job.setReducerClass(MatrixReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
