package edu.nju.jetmuffin.recommender

import edu.nju.jetmuffin.linalg.Matrix
import edu.nju.jetmuffin.spark.SparkEnv
import org.apache.spark.mllib.linalg.distributed.{RowMatrix}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors}
/**
  * Created by cj on 2017/2/15.
  */

/**
  * Recommender using Alternating Least Squares (ALS) from Spark's MLlib
  */
class ALSRecommender() extends Recommender {
  val sc = SparkEnv.sc

  /**
    * Training model of als algorithm
    *
    * @param ratings User history ratings
    * @return
    */
  protected def train(ratings: RDD[Rating], implicitPrefs: Boolean = false) = {
    if (implicitPrefs) {
      ratings.map(x => Rating(x.user, x.product, x.rating - 2.5))
    }

    val numRatings = ratings.count()
    val numUsers = ratings.map(_.user).distinct().count()
    val numMovies = ratings.map(_.product).distinct().count()

    println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")

    // Split input data to training data (80%) and testing data (20%)
    val splits = ratings.randomSplit(Array(0.8, 0.2), seed = 111l)
    val training = splits(0).cache()
    val testing = if (implicitPrefs) {
      splits(1).map(x => Rating(x.user, x.product, if (x.rating > 0) 1.0 else 0.0))
    } else {
      splits(1)
    }.cache()

    val numTraining = training.count()
    val numTesting = testing.count()
    println(s"Training: $numTraining, testing: $numTesting")

    // Parameter adjustment
    val evaluations =
      for (rank     <- Array(10, 25, 50);
           lambda   <- Array(1.0, 0.1, 0.01);
           numIters <- Array(10, 20))
        yield {
          val model = ALS.train(training, rank, numIters, lambda)
          val rmse = computeRMSE(model, testing, implicitPrefs)
          unpersist(model)
          println(s"RMSE = $rmse (rank: $rank, lambda: $lambda, iteration: $numIters).")
          ((rank, lambda, numIters), rmse)
        }

    val ((rank, lambda, numIters), rmse) = evaluations.sortBy(_._2).head
    println(s"After parameter adjust, the best rmse = $rmse (rank: $rank, lambda: $lambda, iteration: $numIters).")

    // Train model on the whole data set
    model = ALS.train(ratings, rank, numIters, lambda)
  }

  /**
    * Recomendation for users based on previous ratings
    *
    * @param ratings                     Previous ratings
    * @param numberOfRecommendedProducts Number of recommended products
    * @return Ratings with recommended products
    */
  override def recommendForUsers(ratings: RDD[Rating], numberOfRecommendedProducts: Int): RDD[(Int, Array[Rating])] = {
    if (model == null) {
      train(ratings)
    }
    val recommendations = this.model.recommendProductsForUsers(numberOfRecommendedProducts)

    recommendations
  }

  /**
    * Recomendation for similar products based on previous ratings
    *
    * @param ratings                     Previous ratings
    * @param numberOfRecommendedProducts Number of recommended products
    * @return List(itemId, List(itemId, similarity)) List of recommend products
    */
  override def recommendForProducts(ratings: RDD[Rating], numberOfRecommendedProducts: Int): RDD[(Int, Array[(Int, Double)])] = {
    if (model == null) {
      train(ratings)
    }

    val similarityMatrix = cosineSimilarityMatrix(model)

    val recommends = similarityMatrix.map {
      case (item, sims) =>
        val sortedSims = sims.sortWith(_._2 > _._2)
        val products = sortedSims.take(numberOfRecommendedProducts)
        (item, products)
    }

    recommends
  }

  /**
    * Output similarity of all pairs of products in model
    *
    * @param model Matrix factorization model
    * @return
    */
  def cosineSimilarityMatrix(model: MatrixFactorizationModel): RDD[(Int, Array[(Int, Double)])] = {
    val items = model.productFeatures.zipWithIndex().map(x => (x._2.toInt, x._1._1)).collectAsMap()

    val rows = model.productFeatures.map(f => Vectors.dense(f._2))
    val mat = Matrix.transposeRowMatrix(new RowMatrix(rows))

    val sims = mat.columnSimilarities().entries.flatMap(
      x => List((x.i.toInt, (x.j.toInt, x.value)), (x.j.toInt, (x.i.toInt, x.value)))
    )
      .map {
        case (item1, (item2, sim)) =>
          val id1 = items(item1)
          val id2 = items(item2)
          (id1, Array((id2, sim)))
      }
      .reduceByKey(_ ++ _)

    sims
  }

  /**
    * Compute root mean square error of model after training
    * @param model
    * @param data
    * @param implicitPrefs
    * @return
    */
  def computeRMSE(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Boolean) = {
    val real = data.map(x => ((x.user, x.product), x.rating))
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map {
      x =>
        val rating = if (implicitPrefs) math.max(math.min(x.rating, 1.0), 0.0) else x.rating
        ((x.user, x.product), rating)
    }
      .join(real).values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }

  /**
    * Unpersist model to free memory
    * @param model
    */
  def unpersist(model: MatrixFactorizationModel): Unit = {
    model.userFeatures.unpersist()
    model.productFeatures.unpersist()
  }
}