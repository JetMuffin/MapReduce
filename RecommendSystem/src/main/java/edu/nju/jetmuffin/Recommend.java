package edu.nju.jetmuffin;

import org.apache.hadoop.conf.Configuration;


/**
 * Created by cj on 2017/2/12.
 */
public class Recommend {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        conf.set("mongo.input.uri", "mongodb://114.212.84.227:27017/douban.movie_comments");
//        conf.set("mongo.output.uri", "mongodb://114.212.84.227:27017/douban.movie_recommend");
        conf.set("input", "RecommendSystem/model.csv");
        conf.set("output", "RecommendSystem/result");
        conf.setInt("topk", 10);
        conf.set("step1", "RecommendSystem/step1");
        conf.set("step2", "RecommendSystem/step2");
        conf.set("step3_1", "RecommendSystem/step3_1");
        conf.set("step3_2", "RecommendSystem/step3_2");

        UserPreference.run(conf);
        CoocurrentMatrix.run(conf);
        MatrixMultiply.run(conf);
        ScoreRanking.run(conf);
    }
}
