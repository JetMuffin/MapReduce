package edu.nju.jetmuffin.recommender

import edu.nju.jetmuffin.linalg.Matrix
import edu.nju.jetmuffin.spark.SparkEnv
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
/**
  * Created by cj on 2017/2/15.
  */

/**
  * Recommender using Alternating Least Squares (ALS) from Spark's MLlib
  * @param rank Rank of the feature matrices
  * @param lambda The regularization parameter
  * @param numIters The number of iterations to run
  */
class ALSRecommender(rank: Int, lambda: Double, numIters: Int) extends Recommender {
  val sc = SparkEnv.sc

  /**
    * Training model of als algorithm
    * @param ratings User history ratings
    * @param userRatings Products recommend for users to evaluate the model
    * @return
    */
  protected def train(ratings: RDD[Rating], userRatings: Option[RDD[Rating]] = None) = {
    val trainingData = userRatings match {
      case Some(rdd) => ratings.union(rdd)
      case None => ratings
    }
    trainingData.persist

    model = ALS.train(trainingData, rank, numIters, lambda)
  }

  /**
    * Recomendation for users based on previous ratings
    *
    * @param ratings                     Previous ratings
    * @param numberOfRecommendedProducts Number of recommended products
    * @return Ratings with recommended products
    */
  override def recommendForUsers(ratings: RDD[Rating], numberOfRecommendedProducts: Int): RDD[(Int, Array[Rating])] = {
    if(model == null) {
      train(ratings)
    }
    val recommendations = this.model.recommendProductsForUsers(numberOfRecommendedProducts)

    return recommendations
  }

  /**
    * Recomendation for similar products based on previous ratings
    * @param ratings Previous ratings
    * @param numberOfRecommendedProducts Number of recommended products
    * @return List(itemId, List(itemId, similarity)) List of recommend products
    */
  override def recommendForProducts(ratings: RDD[Rating], numberOfRecommendedProducts: Int): RDD[(Int, Array[(Int, Double)])] = {
    if(model == null) {
      train(ratings)
    }

    val similarityMatrix = CosineSimilarityMatrix(model)

    val recommends = similarityMatrix.map {
      case (item, sims) =>
        val sortedSims = sims.sortWith(_._2 > _._2)
        val products = sortedSims.take(numberOfRecommendedProducts)
        (item, products)
    }

    recommends
  }

  def CosineSimilarityMatrix(model: MatrixFactorizationModel) : RDD[(Int, Array[(Int, Double)])] = {
    val items = model.productFeatures.zipWithIndex().map(x => (x._2.toInt, x._1._1)).collectAsMap()

    val rows = model.productFeatures.map(f => Vectors.dense(f._2))
    val mat = Matrix.transposeRowMatrix(new RowMatrix(rows))

    val sims = mat.columnSimilarities().entries.flatMap(
      x => List((x.i.toInt, (x.j.toInt, x.value)), (x.j.toInt, (x.i.toInt, x.value)))
    )
      .map {
        case(item1, (item2, sim)) =>
          val id1 = items(item1)
          val id2 = items(item2)
          (id1, Array((id2, sim)))
      }
      .reduceByKey(_++_)

    sims
  }
}
