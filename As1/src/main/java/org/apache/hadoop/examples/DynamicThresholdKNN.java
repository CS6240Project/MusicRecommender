package org.apache.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.*;

public class DynamicThresholdKNN {

    // Mapper 将输入的项目对相似度转换为 (ItemA, ItemB:Similarity) 和 (ItemB, ItemA:Similarity) 格式
    public static class KNNMapper extends Mapper<Object, Text, Text, Text> {
        private Text itemKey = new Text();
        private Text similarityValue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t"); // 拆分项目对和相似度
            if (line.length < 2) return; // 检查格式
            String items = line[0].replaceAll("[()]", ""); // 移除括号
            String[] itemPair = items.split(",");
            if (itemPair.length < 2) return;

            String itemA = itemPair[0].trim();
            String itemB = itemPair[1].trim();
            String similarity = line[1].trim();

            // 发出两对键值：(ItemA, ItemB:Similarity) 和 (ItemB, ItemA:Similarity)
            itemKey.set(itemA);
            similarityValue.set(itemB + ":" + similarity);
            context.write(itemKey, similarityValue);

            itemKey.set(itemB);
            similarityValue.set(itemA + ":" + similarity);
            context.write(itemKey, similarityValue);
        }
    }

    // Reducer 收集每个项目的相似项目并保留相似度高于阈值的项目
    public static class KNNReducer extends Reducer<Text, Text, Text, Text> {
        private static final double THRESHOLD = 0.75; // 设置相似度阈值
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Pair<Double, String>> neighbors = new ArrayList<>();

            // 遍历相似度数据，过滤出大于或等于阈值的邻居
            for (Text value : values) {
                String[] itemSimilarity = value.toString().split(":");
                if (itemSimilarity.length < 2) continue;

                String item = itemSimilarity[0];
                double similarity = Double.parseDouble(itemSimilarity[1]);

                // 只保留相似度大于或等于阈值的邻居
                if (similarity >= THRESHOLD) {
                    neighbors.add(Pair.of(similarity, item));
                }
            }

            // 将符合阈值的邻居排序
            neighbors.sort((a, b) -> Double.compare(b.getLeft(), a.getLeft()));

            // 格式化输出邻居列表
            StringBuilder knnResult = new StringBuilder();
            for (Pair<Double, String> neighbor : neighbors) {
                knnResult.append(neighbor.getRight()).append(":").append(neighbor.getLeft()).append(",");
            }

            // 去掉最后一个逗号
            if (knnResult.length() > 0) {
                knnResult.setLength(knnResult.length() - 1);
            }

            result.set(knnResult.toString());
            context.write(key, result); // 输出 (Item, Neighbor List with Threshold Filtering)
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: DynamicThresholdKNN <in> <out>");
            System.exit(2);
        }

        // 检查输出路径是否存在，若存在则删除
        Path outputPath = new Path(otherArgs[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "Dynamic Threshold KNN Calculation");
        job.setJarByClass(DynamicThresholdKNN.class);
        job.setMapperClass(KNNMapper.class);
        job.setReducerClass(KNNReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
