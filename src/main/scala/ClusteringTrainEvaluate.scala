import java.net.URI
import java.util

import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.util.LongAccumulator
import org.apache.spark.mllib.linalg.Vectors

import scala.util.Random


object ClusteringTrainEvaluate {

  def main(args: Array[String]) {

    //configuration and initialization of model
    val conf = new SparkConf().setAppName("ClusteringTrainEvaluate")
    val ssc = new StreamingContext(conf, Seconds(1))
    val sc = ssc.sparkContext
     ssc.sparkContext.setLogLevel("WARN")
    val coresetSize = args(0).toInt
    val kOption = args(1).toInt
    val dim = args(2).toInt
    val trainPath = args(3)
    
    val acum = ssc.sparkContext.longAccumulator
    //convert doubles to Example points
    val trainExamples = ssc.textFileStream(trainPath).map(arr => {
      val tokens = arr.split(",")
      new Example(new DenseInstance(tokens.map(_.toDouble)))
    })
   
    //create coreset sized subsets as key,value pairs (Int,Array[Example])
    val examples = trainExamples.map(transformRDD(_,acum,coresetSize)).reduceByKey(_++_)
    //create sub coresets for each subset flatten result and convert to Vector for mllib train
    val coreset = examples.flatMap(p => {
      val coresetRDD =  mergeReduceCoreset(p, coresetSize)
        val coresetVector =  coresetRDD.map(x=>{
           val tokens = x.in.getFeatureIndexArray().map(_._1)
           val point = Vectors.dense(tokens)
           point
         })
      coresetVector
    })
  
  //init mllib model StreamingKMeans
   val alg = new StreamingKMeans()
     .setK(kOption)
     .setDecayFactor(1.0)
     .setInitialCenters(Array.fill(kOption)(Vectors.dense(Array.fill(dim)(Random.nextGaussian()))),Array.fill(kOption)(0.0))

    //train model
     alg.trainOn(coreset)
    //print centers
     coreset.foreachRDD { (rdd, time) =>
       var model = alg.latestModel
       val centers = model.clusterCenters
       println("Time : "+time+" centers = "+centers.toSeq)
     }

    //start the loop
    ssc.start()
    ssc.awaitTermination()
  }

   //for every coreset sized input value give a key
  //merge each key into an array
  def transformRDD(point: Example, acum: LongAccumulator, coresetSize:Int): (Int,Array[Example])={
    val cnt = Math.floor(acum.value / (coresetSize)).toInt
    acum.add(1)
    (cnt,Array(point))
}
//call makeCoreset for every subset. 
  //extract subcoreset 
  //  If C1 and C2 are each (k,ε)-coresets for disjoint multi-sets P1 and P2 respectively,
  // then C1 U C2 is a (k,ε )-coreset for P1 U P2.
  def mergeReduceCoreset(rdd: (Int, Array[Example]), coresetSize: Int): Array[Example] = {
     val resultCor = makeCoreset(rdd._2,(coresetSize)/10)
     resultCor
  }

  def makeCoreset(cor:  Array[Example], coresetSize: Int): Array[Example] = {
    val tree = new TreeCoreset
    val coreset = tree.retrieveCoreset(tree.buildCoresetTree(cor, coresetSize), new Array[Example](0))
    coreset
  }

}
