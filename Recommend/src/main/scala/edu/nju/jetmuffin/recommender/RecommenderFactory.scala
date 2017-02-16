package edu.nju.jetmuffin.recommender

import edu.nju.jetmuffin.cmd.Conf

/**
  * Created by cj on 2017/2/15.
  */
object RecommenderFactory {
  private var recommenderFactories: List[GeneralRecommenderFactory] =
    List(
      ItemCFRecommenderFactory,
      ALSCFRecommenderFactory
    )

  /**
    * Trait of general recommender factory
    */
  trait GeneralRecommenderFactory {
    def getName: String

    def getRecommender(conf: Conf): Recommender
  }

  /**
    * Recommender factory of item-based collaborative filtering algorithm
    */
  object ItemCFRecommenderFactory extends GeneralRecommenderFactory {
    override def getName = "itemcf"

    override def getRecommender(conf: Conf): Recommender = {
      new ItemCFRecommender()
    }
  }

  /**
    * Recommender factory of alternating least squares algorithm
    */
  object ALSCFRecommenderFactory extends GeneralRecommenderFactory {
    override def getName = "als"

    override def getRecommender(conf: Conf): Recommender = {
      val rank = conf.getInt("als_rank", 50)
      val lambda = conf.getDouble("als_lambda", 0.01)
      val numIters = conf.getInt("als_iteration", 10)

      new ALSRecommender(rank, lambda, numIters)
    }
  }

  /**
    * Returns an instance of Recommender
    * @param conf Instance of Conf
    * @return Recommender instance
    */
  def getRecommenderInstance(conf: Conf): Recommender = {
    val recommenderName = conf.get("recommender", "als")
    val recommenderFactoriesMap = recommenderFactories.map(rec => rec.getName -> rec).toMap
    val recommender = recommenderFactoriesMap.get(recommenderName)

    recommender match {
      case Some(rec) => rec.getRecommender(conf)
      case None => throw new RecommenderNotFoundException
    }
  }

  class RecommenderNotFoundException extends Exception
}
