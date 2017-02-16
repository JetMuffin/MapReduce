package edu.nju.jetmuffin.recommender

import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

/**
  * Created by cj on 2017/2/15.
  */

/**
  * Trait for Recommenders
  */
trait Recommender {
  var model: MatrixFactorizationModel = null

  /**
    * Recomendation of products based on previous ratings
    * @param ratings Previous ratings
    * @param numberOfRecommendedProducts Number of recommended products
    * @return Ratings with recommended products
    */
  def recommendForUsers(ratings: RDD[Rating], numberOfRecommendedProducts: Int = 5): RDD[(Int, Array[Rating])]

  def recommendForProducts(ratings: RDD[Rating], numberOfRecommendedProducts: Int = 5): RDD[(Int, Array[(Int, Double)])]
}
