/**
 * @author Hengte Lin
 */

package edu.gatech.cse8803.main

import java.text.SimpleDateFormat


import edu.gatech.cse8803.features.FeatureConstruction
import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.model.{PatientInfo, Diagnostic, Medication}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}



object Main {
  def main(args: Array[String]) {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = createContext
    val sqlContext = new SQLContext(sc)

    println(new java.io.File("data/data").exists)
    if (! new java.io.File("data/data").exists) {
      /** initialize loading of data */
      val (medication, patientResult, diagnostic) = loadRddRawData(sqlContext)
      /** conduct phenotyping */
      val featureTuples = sc.union(
        FeatureConstruction.constructDiagnosticFeatureTuple(diagnostic),
        FeatureConstruction.constructAgeFeatureTuple(patientResult),
        FeatureConstruction.constructGenderFeatureTuple(patientResult),
        FeatureConstruction.constructMedicationFeatureTuple(medication)
      )
      CSVUtils.saveFeatureToLocal(featureTuples)
    }




    //--------------------------------------------------------------------------
/*    val loadedfeaturetuple=CSVUtils.readFeatureFromLocal(sc,"data/data")
    var pSet=Set("391","391.9","392","394","395","396","397","398","401","401.0","401.1","401.9","402","403","403.0","403.1","404","405","405.0","405.01","405.1","405.11","410","410.0","410.1","410.2","410.3","410.4","410.5","410.6","410.7","410.8","410.9","411","411.0","411.1","412","413","413.0","413.1","414","414.0","414.1","414.10","414.11","414.12","414.8","414.9","415","415.0","415.1","415.11","415.12","415.19","416","416.0","416.1","416.2","416.8","416.9","417","417.0","417.1","417.8","417.9","420","420.9","420.91","421","421.0","422","422.9","422.91","423","424","424.0","424.1","424.2","424.3","425","425.0","425.1","425.2","425.3","425.4","425.5","425.7","425.8","425.9","426","426.0","426.11","426.12","426.13","426.3","426.4","426.6","426.7","427","427.0","427.3","427.31","427.32","427.4","427.41","427.5","427.6","427.8","427.81","427.89","427.9","428","428.0","428.1","428.2","428.3","428.4","429","429.0","429.1","429.2","429.3","429.4","429.5","429.6","429.7","429.71","429.79","429.8","429.81","429.82","429.83","429.89","429.9","430","431","432","432.9","433","433.0","433.1","433.2","434","434.0","434.00","434.01","434.1","434.10","434.11","435","435.0","435.1","435.2","435.3","435.9","436","437","437.0","437.1","437.2","437.3","437.4","437.5","437.6","437.7","438","438.0","438.1","438.10","438.11","438.12","438.19","438.2","438.20","438.21","438.22","438.3","438.4","438.5","438.8","438.81","438.82","438.83","438.84","438.85","438.9","440","440.1","440.2","440.21","440.23","441","441.0","441.3","441.4","441.9","442","443","443.0","443.1","443.2","443.21","443.22","443.23","443.24","443.29","443.8","443.82","443.9","444","445","446","446.1","446.5","447","447.0","448","449","451","451.1","451.11","451.19","451.8","451.82","451.9","452","453","453.4","453.41","453.42","453.9","454","454.0","454.1","454.2","454.9","455","455.0","455.2","455.3","455.4","455.6","456","456.0","456.1","456.4","457","457.0","458","458.0","458.2","459","459.8","459.81")

    FeatureConstruction.construct(sc, loadedfeaturetuple, pSet)
    return 0*/
    /** feature construction with all features */

    val anchorDict= phenotypehasmap
    val loadedfeaturetuple=CSVUtils.readFeatureFromLocal(sc,"data/data")
    var totalCoefficient:List[(String, String, Double)] = List[(String, String, Double)]()
    for( (phenotype,pSet) <- anchorDict)
    {
      println(phenotype)
      val rawFeatures = FeatureConstruction.construct(sc, loadedfeaturetuple, pSet)
      val coeffient:List[(String,String,Double)]={ if (rawFeatures._1.isEmpty) List((phenotype,"Nan",0.0))
      else trainLogistic(rawFeatures._1,rawFeatures._2).map{case (a:String,b:Double) =>(phenotype,a,b)}
    }

      totalCoefficient= totalCoefficient ::: coeffient
    }

    sc.parallelize(totalCoefficient).saveAsTextFile("data/output")

  }


  def loadRddRawData(sqlContext: SQLContext): (RDD[Medication], RDD[PatientInfo], RDD[Diagnostic]) = {

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")
    val medicationdata=CSVUtils.loadCSVAsTable(sqlContext,"data/prescriptions.csv","MedicationTable")
    val IDdata=CSVUtils.loadCSVAsTable(sqlContext,"data/patients.csv","IDTable")
    val diagnosedata=CSVUtils.loadCSVAsTable(sqlContext,"data/diagnostics.csv","DiagTable")
    val admissiondata=CSVUtils.loadCSVAsTable(sqlContext,"data/admissions.csv","AdTable")


    val RDDrowmed= sqlContext.sql("SELECT subject_id as patientID, drug AS medicine  FROM MedicationTable")
    val RDDrowdiag= sqlContext.sql("SELECT subject_id as patientID, icd9_code AS code  FROM DiagTable")
    val RDDpatient= sqlContext.sql("SELECT t.subject_id as patientID,  t.gender,DATEDIFF(a.admittime,t.dob) as age, a.admittime, t.dob FROM IDTable t INNER JOIN AdTable a ON t.subject_id = a.subject_id")
    val medication: RDD[Medication] = RDDrowmed.map(p => Medication(p(0).asInstanceOf[String],p(1).asInstanceOf[String].toLowerCase))
    val diagnostic: RDD[Diagnostic] =  RDDrowdiag.map(p => Diagnostic(p(0).asInstanceOf[String],p(1).asInstanceOf[String].toLowerCase))
    val patients:RDD[PatientInfo] = RDDpatient.map(p=> PatientInfo(p(0).asInstanceOf[String],p(1).asInstanceOf[String], p(2).asInstanceOf[Int]/3650))

    (medication, patients, diagnostic)
  }




  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Application", "local")


  def phenotypehasmap:Map[String,Set[String]]={
    Map(
      "Alcoholism" -> Set("303","305.00","305.01","305.02"),
      "Anticoagulated" -> Set("790.92","e934.2","v58.61","low molecular weight heparins","anticoagulants - coumarin","thrombin inhibitor - selective direct & reversible","direct factor xa inhibitors","vitamins - k, phytonadione and derivatives","factor ix preparations","factor ix complex (prothrombin complex concentrate) preparations","ffp"),
      "Ankle fracture" -> Set("824"),
      "Bicycle accident" -> Set("e006.4","e800.3","e801.3","e802.3","e803.3","e804.3","e805.3","e806.3","e807.3","e810.6","e811.6","e812.6","e813.6","e814.6","e815.6","e816.6","e817.6","e818.6","e819.6","e820.6","e821.6","e822.6","e823.6","e824.6","e825.6","e826"),
      "Allergic reaction" ->Set("995.3","allergic reaction","allergic rxn"),
      "asthma-copd" -> Set("491","492","493"),
      "Back pain" -> Set("724"),
      "Chest pain" ->Set("410","411.1","413","786.50","786.59","428"),
      "Cellulitis" -> Set("680","681","682","683","684","685","686"),
      "Congestive heart failure " -> Set("428"),
      "Cholecystitis" -> Set("574","575"),
      "Cerebrovascular accident" -> Set("434","435","436","437.8","437.9","thrombolytic - tissue plasminogen activators"),
      "Diabetes" -> Set("250","diabetic therapy"),
      "Employee exposure" -> Set("e920.5"),
      "Epistaxis " -> Set("784.7"),
      "Gastranteritis" -> Set("008","558","787.91"),
      "Gastranteritis Bleed"->Set("569.3","578"),
      "Headache" -> Set("339","346","784.0"),
      "Hematuria" ->Set("599.70","599.71","599.7"),
      "Intracerebral hemorrhage" -> Set("430","431","432","852","853"),
      "Chest Pain" -> Set("01. infectious and parasitic diseases","038","460-466","480-488","540-543","562.11","575.0","576.1","590","595.0","599.0","680-686","tissue","790.7","995.91","995.92","cephalosporin antibiotics","macrolide antibiotics and combinations","glycopeptide antibiotics","fluoroquinolone antibiotics"),
      "Deep vein thrombosis" -> Set("453.40","453.41","453.42","453.82","453.83"),
      "Kidney stone" -> Set("592","788.0"),
      "Liver (history)" -> Set("571","572.2"),
      "Motor Vehicle Accident" -> Set("e810","e811","e812","e813","e814","e815","e816","e817","e818","e819"),
      "Pancreatitis" -> Set("577.0"),
      "Pneumonia" -> Set("480","481","482","483","484","","485","486"),
      "Psych" -> Set("295","296","297","298","311","v62.84","v62.85"),
      "Obstruction " -> Set("560.9"),
      "Septic shock" -> Set("785.52","cardiac sympathomimetics"),
      "Severe sepsis" -> Set("785.52","995.92","cardiac sympathomimetics"),
      "Sexual assault" -> Set("v71.5"),
      "Suicidal ideation" ->Set("v62.84","si","suicidal ideation"),
      "Syncope" -> Set("780.2","syncopal episode"),
      "Uti" -> Set("590","599.0"),
      "Cardiac etiology" -> Set("410","411","ischemic heart disease","413","428","785.51","antianginal - coronary vasodilators (nitrates)","diuretic - loop","antianginal - coronary vasodilators (nitrates)","combinations"),
      "Abdominal pain"-> Set("540","541","542","543","560.0","560.2","560.89","560.9","562.01","562.03","562.11","562.13","574","575.0","575.10","576.1","577.0","789.00","789.01","789.02","789.03","789.04","789.05","789.06","789.07","789.09","789.0","789.60","789.61","789.62","789.63","789.64","789.65","789.66","789.67","789.69")
    )
  }

  def trainLogistic(patientrdd: RDD[LabeledPoint],IDMap: scala.collection.Map[String, Long]): List[(String,Double)] ={
    patientrdd.cache()
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(patientrdd)
    val coefficients=model.weights.toArray.zipWithIndex.sortBy(_._1).reverse.take(20).map(f => (IDMap.find(x=> x._2==f._2).getOrElse(("Nan",0))._1,f._1))

    coefficients.toList
  }
}
