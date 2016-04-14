/**
 * @author Hengte Lin
 */
package edu.gatech.cse8803.ioutils


import java.io.File
import edu.gatech.cse8803.features.FeatureConstruction.FeatureTuple

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.SQLContext
import com.databricks.spark.csv.CsvContext



object CSVUtils {
  def loadCSVAsTable(sqlContext: SQLContext, path: String, tableName: String): SchemaRDD = {
    val data = sqlContext.csvFile(path)
    data.registerTempTable(tableName)
    data
  }

  def saveFeatureToLocal( feature:RDD[FeatureTuple]): Unit ={
    val file="data/data"
    new File(file).delete()

    println("write")
    val csvfeature=feature.map(f=>f._1+"$"+f._2+"$"+f._3.toString).repartition(1)
    csvfeature.saveAsTextFile(file)

  }

  def readFeatureFromLocal(sc: SparkContext, path: String):RDD[FeatureTuple]={


    val rawdata=sc.textFile(path)
      .map(f=>f.split('$'))
      .map{
        case Array(b:String,c:String,d:String,_*)  => (b.toString, c.toString, d.toDouble)
        case a => ("Nan", "Nan", 0.0)
      }
    //rawdata.filter(_._2.size==3).map(_._2).distinct().take(20).foreach(println)
    //rawdata.take(1)(0).foreach(println)
    //rawdata.map(f => f.).distinct().take(50).foreach(println)
    /*rawdata.filter(_.size==8).take(50).foreach{
      println()
      _.foreach{
        print("(")
        print(_)

      }
    }*/
    //(rawdata.take(10)).foreach(println)

    rawdata
  }

  def loadCSVAsTable(sqlContext: SQLContext, path: String): SchemaRDD = {
    loadCSVAsTable(sqlContext, path, inferTableNameFromPath(path))
  }

  private val pattern = "(\\w+)(\\.csv)?$".r.unanchored
  def inferTableNameFromPath(path: String) = path match {
    case pattern(filename, extension) => filename
    case _ => path
  }


}
