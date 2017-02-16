package edu.nju.jetmuffin.recommender

import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

/**
  * Created by cj on 2017/2/15.
  */
class ItemCFRecommender extends Recommender {

  /**
    * Recomendation of products based on previous ratings
    *
    * @param ratings                     Previous ratings
    * @param numberOfRecommendedProducts Number of recommended products
    * @return Ratings with recommended products
    */
  override def recommendForUsers(ratings: RDD[Rating], numberOfRecommendedProducts: Int): RDD[(Int, Array[Rating])] = {
    // TODO: implementation
    return null
  }

  override def recommendForProducts(ratings: RDD[Rating], numberOfRecommendedProducts: Int): RDD[(Int, Array[(Int, Double)])] = ???
}
