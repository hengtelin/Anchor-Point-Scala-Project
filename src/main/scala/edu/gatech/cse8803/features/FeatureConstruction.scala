/**
 * @author Hengte Lin
 */
package edu.gatech.cse8803.features

import breeze.linalg.max
import edu.gatech.cse8803.model.{PatientInfo, LabResult, Medication, Diagnostic}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD


object FeatureConstruction {


  /**
   * (patient-id, feature-name, feature-value)
   */
  type FeatureTuple = (String, String, Double)

  /**
   * Aggregate feature tuples from diagnostic
    *
    * @param diagnostic RDD of diagnostic
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    diagnostic.map(f => (f.patientID,f.code,1.0)).distinct()

  }

  /**
   * Aggregate feature tuples from medication
    *
    * @param medication RDD of medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    medication.map(f => (f.patientID,f.medicine,1.0) ).distinct()
  }

  /**
   * Aggregate feature tuples from patient result
    *
    * @param patientResult RDD of lab result
   * @return RDD of feature tuples
   */
  def constructAgeFeatureTuple(patientResult: RDD[PatientInfo]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    patientResult.map(f =>(f.patientID,"Age",f.age.toDouble)).keyBy(_._1).reduceByKey((f1,f2)=> (f1._1,f2._2,max(f1._3,f2._3))).map(_._2)
  }
  def constructGenderFeatureTuple(patientResult: RDD[PatientInfo]): RDD[FeatureTuple] ={
    patientResult.map(
      f =>{ if(f.gender=="M")  (f.patientID,"Sex",1.0)  else (f.patientID,"Sex",0.0)}
    ).distinct()
  }

  /**
   * Given a feature tuples RDD, construct features in vector
   * format for each patient. feature name should be mapped
   * to some index and convert to sparse feature format.
    *
    * @param sc SparkContext to run
   * @param feature RDD of input feature tuples
   * @return
   */
  def construct(sc: SparkContext, feature: RDD[FeatureTuple], anchorDictSet:Set[String]): (RDD[LabeledPoint],scala.collection.Map[String,Long]) = {

    /** save for later usage */
    feature.cache()
    val trueset=feature.filter(f => anchorDictSet.contains(f._2)).map(_._1).collect().toSet
    println("trueset")
    println(trueset.size)
    //sc.parallelize(trueset.toList).repartition(1).saveAsTextFile("PatientSet")
    if (trueset.size==0) return (sc.emptyRDD,Map[String,Long](""->0L))
    println("passed")
    val restfeature=feature.filter(f => !anchorDictSet.contains(f._2))
    /** create a feature name to id map*/
    val idmap=restfeature.map(f => f._2).distinct().zipWithIndex().collectAsMap()

    val bidmap=sc.broadcast(idmap)
    val idnum=bidmap.value.size


    /** transform input feature */
    val result=restfeature
      .map(f => (f._1,bidmap.value(f._2),f._3) )
      .groupBy(_._1)
      .map(f => {
          val featurelist=f._2.toList.map(x =>(x._2.toInt,x._3.toDouble))
          val label={if (trueset.contains(f._1)) 1 else 0}
        LabeledPoint(label,Vectors.sparse(idnum,featurelist))
    })

    (result,idmap)

  }

}


