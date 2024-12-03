package org.apache.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class ItemSimilarity {

    // Mapper 将每个用户的评分数据转换为 (ItemID, UserID:Score) 格式
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {

        private Text itemId = new Text();
        private Text userRating = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] userRatings = value.toString().split("\\s+");
            String userId = userRatings[0];  // 提取用户ID

            // 过滤掉 UserID >= 10000 的数据
            try {
                int userIdInt = Integer.parseInt(userId);
//                if (userIdInt >= 1000) {
//                    return;  // 跳过 UserID >= 10000 的行
//                }
            } catch (NumberFormatException e) {
                System.out.println("Invalid user id: " + userId);
                // 跳过格式错误的行
                return;
            }

            for (int i = 1; i < userRatings.length; i++) {
                String[] itemScore = userRatings[i].replaceAll(",", "").split(":");  // 去除尾标逗号
                if (itemScore.length < 2) continue;  // 跳过格式错误的行
                itemId.set(itemScore[0]);  // 设置项目ID
                userRating.set(userId + ":" + itemScore[1]);  // 设置UserID:Score
                context.write(itemId, userRating);  // 输出 (ItemID, UserID:Score)
            }
        }
    }

    // Reducer 接收同一项目的所有用户评分数据，计算项目对的皮尔逊相关系数
    public static class SimilarityReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private DoubleWritable similarityScore = new DoubleWritable();
        private List<Song> songList = new ArrayList<Song>();

        class Song {
            private String id;
            private HashMap<String, Double> usersRating;

            public String getId() {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }

            public Song(String id) {
                this.id = id;
                this.usersRating = new HashMap<>();
            }


            public HashMap<String, Double> getUsersRating() {
                return usersRating;
            }

            public void setUsersRating(HashMap<String, Double> usersRating) {
                this.usersRating = usersRating;
            }

            public double getRelated(Song b) {
                List<Pair<Double, Double>> aRatingAndbRating = new LinkedList<>();

                // 获取共同评分的用户
                for (String user : usersRating.keySet()) {
                    if (b.getUsersRating().containsKey(user)) {
                        aRatingAndbRating.add(Pair.of(this.getUsersRating().get(user), b.getUsersRating().get(user)));
                    }
                }

                int n = aRatingAndbRating.size();  // 共同评分用户的数量
                if (n == 0) {
                    return 0.0; // 如果没有共同评分用户，返回 null 或者处理无交集的情况
                }

                // 计算歌曲 A 和歌曲 B 的平均评分
                double sumA = 0.0;
                double sumB = 0.0;
                for (Pair<Double, Double> pair : aRatingAndbRating) {
                    sumA += pair.getLeft(); // 歌曲 A 的评分
                    sumB += pair.getRight(); // 歌曲 B 的评分
                }

                double meanA = sumA / n;
                double meanB = sumB / n;

                // 计算皮尔逊相关系数
                double numerator = 0.0; // 分子
                double denominatorA = 0.0; // 歌曲 A 的平方差
                double denominatorB = 0.0; // 歌曲 B 的平方差

                for (Pair<Double, Double> pair : aRatingAndbRating) {
                    double diffA = pair.getLeft() - meanA; // 歌曲 A 的评分偏差
                    double diffB = pair.getRight() - meanB; // 歌曲 B 的评分偏差

                    numerator += diffA * diffB; // 分子
                    denominatorA += diffA * diffA; // 歌曲 A 的平方差
                    denominatorB += diffB * diffB; // 歌曲 B 的平方差
                }

                double denominator = Math.sqrt(denominatorA) * Math.sqrt(denominatorB); // 分母

                if (denominator == 0) {
                    return 0.0; // 避免除零错误，如果分母为 0，返回 null 或者其他值
                }

                return numerator / denominator; // 计算并返回皮尔逊相关系数
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 收集当前歌曲（key）对应的所有用户评分
            Map<String, Double> userRatingsMap = new HashMap<>();
            Song currentSong = new Song(key.toString());

            // 获取所有评分并将其放入当前歌曲的评分数据中
            for (Text val : values) {
                String[] parts = val.toString().split(":");
                if (parts.length < 2) continue;  // 跳过格式错误的行
                currentSong.getUsersRating().put(parts[0], Double.valueOf(parts[1]));
            }

            // 将当前歌曲存入 songList，用于与其他歌曲计算相似度
            songList.add(currentSong);
        }

        // 在 cleanup() 中计算所有歌曲对之间的皮尔逊相关系数
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // 遍历 songList，计算所有歌曲之间的皮尔逊相关系数
            for (int i = 0; i < songList.size(); i++) {
                for (int j = i + 1; j < songList.size(); j++) {
                    Song songA = songList.get(i);
                    Song songB = songList.get(j);

                    // 计算 songA 和 songB 之间的皮尔逊相关系数
                    double similarity = songA.getRelated(songB);

                    // 只输出相似度大于 0 的歌曲对
                    if (similarity > 0) {
                        // 输出两首歌及其相似度
                        context.write(new Text("(" + songA.getId() + ", " + songB.getId() + ")"), new DoubleWritable(similarity));
                    }
                }
            }
        }


    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: ItemSimilarity <in> <out>");
            System.exit(2);
        }

        // 检查输出路径是否存在，若存在则删除
        Path outputPath = new Path(otherArgs[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "Item Similarity Calculation");
        job.setJarByClass(ItemSimilarity.class);
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(SimilarityReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
