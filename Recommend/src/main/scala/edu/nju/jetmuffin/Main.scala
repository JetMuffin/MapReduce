package edu.nju.jetmuffin

import edu.nju.jetmuffin.cmd.Conf
import edu.nju.jetmuffin.data.Dataset
import edu.nju.jetmuffin.recommender.RecommenderFactory

/**
  * Created by cj on 2017/2/15.
  */
object Main extends App {
  val conf = new Conf(Some("/Users/cj/workspace/java/MapReduce/Recommend/src/main/resources/config.properties"))

  val recommender = RecommenderFactory.getRecommenderInstance(conf)
  val ratings = Dataset.getRatings(conf)
  val existingProductsSet = ratings.map(rating => (rating.user, rating.product)).collect().toSet

//  val result = recommender.recommendForUsers(ratings).map {
//    case(user, recomendations) => {
//      val filteredRecommendations = recomendations.filterNot(x => existingProductsSet.contains((x.user, x.product)))
//
//      var str = ""
//      for(r <- filteredRecommendations) {
//        str += r.product + ":" + r.rating + " "
//      }
//      if (str.endsWith(" ")) {
//        str = str.substring(0, str.length - 1)
//      }
//
//      (user, str)
//    }
//  }
//
  val result = recommender.recommendForProducts(ratings).map {
    case(item, recomendations) => {
      var str = ""
      for(r <- recomendations) {
        str += r._1 + ":" + r._2 + " "
      }
      if (str.endsWith(" ")) {
        str = str.substring(0, str.length - 1)
      }

        (item, str)
    }
  }

  result.coalesce(1).sortByKey().saveAsTextFile(conf.get("data_dir", "data") + "/" + conf.get("output", "result"))
}
