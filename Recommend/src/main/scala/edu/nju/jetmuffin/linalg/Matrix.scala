package edu.nju.jetmuffin.linalg

import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
  * Created by cj on 2017/2/16.
  */

/**
  * Transpose a row matrix
  */
object Matrix {
  def transposeRowMatrix(m: RowMatrix): RowMatrix = {
    val transposedRowsRDD = m.rows.zipWithIndex.map {
      case (row, rowIndex) => rowToTransposedTriplet(row, rowIndex)
    }
      .flatMap(x => x)
      .groupByKey
      .sortByKey()
      .map(_._2)
      .map(buildRow)
    new RowMatrix(transposedRowsRDD)
  }

  def rowToTransposedTriplet(row: Vector, rowIndex: Long): Array[(Long, (Long, Double))] = {
    val indexedRow = row.toArray.zipWithIndex
    indexedRow.map {
      case (value, colIndex) => (colIndex.toLong, (rowIndex, value))
    }
  }

  def buildRow(rowWithIndexes: Iterable[(Long, Double)]): Vector = {
    val resArr = new Array[Double](rowWithIndexes.size)
    rowWithIndexes.foreach {
      case (index, value) => resArr(index.toInt) = value
    }
    Vectors.dense(resArr)
  }
}
