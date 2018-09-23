import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

import scala.util.Random

object ClusteringTrainEvaluate {

  def main(args: Array[String]) {

    //configuration and initialization of model
    val conf = new SparkConf().setAppName("ClusteringTrainEvaluate")

    val ssc = new StreamingContext(conf, Seconds(1))
      ssc.sparkContext.setLogLevel("WARN")
    val coresetSize = args(0).toInt
    val kOption = args(1).toInt
    val dim = args(2).toInt
    val trainPath = args(3).toString
   // val testPath = args(4).toString

    val trainExamples = ssc.textFileStream(trainPath).map(arr => {
      val tokens = arr.split(",")
      new Example(new DenseInstance(tokens.map(_.toDouble)))
    })

  /*  val testExamples =ssc.textFileStream(testPath)
       .map(arr => {
         val coord: String = "("+Random.nextInt(kOption)+",["+arr+"])"
             coord
       }).map(LabeledPoint.parse)
  */
    val examples = trainExamples.transform(transformInput(_,ssc,coresetSize))
    val coreset = examples.transform(rdd => {
      init(rdd, coresetSize)
    })

  /* coreset.foreachRDD(p => p.foreach(pr => {
      println("resulted coreset ")
      pr._2.foreach(prr => println("point: " + prr.in))
      println("end printing")
    }))
*/
    //convert resulted coreset's format to vector for training
    val coresetVector = coreset.map(c=>c._2)
      .flatMap(line => line)
      .map(e => {
        val p = "[" + e.in.toString + "]"
        p
    }).map(Vectors.parse)
    
 //train mllib model 
    val model = new StreamingKMeans()
          .setK(kOption)
          .setDecayFactor(1.0)
          .setRandomCenters(dim, 0.0)
   model.trainOn(coresetVector)
  //assign input to clusters
  val assigned = coresetVector.map(ex=> {
      val clusters = model.latestModel().clusterCenters//.map(p => new Example(new DenseInstance(p.toArray)))
      val assignedCl = clusters.foldLeft((0, Double.MaxValue, 0))(
        (cl, centr) => {
          val dist =  Vectors.sqdist(ex,centr)
          if (dist < cl._2) ((cl._3, dist, cl._3 + 1))
          else ((cl._1, cl._2, cl._3 + 1))
        })._1
      (assignedCl,ex)
    })
    //print results
    assigned.print(100)
    coresetVector.foreachRDD((rdd,time) =>{
      println(s"time: $time")
      println("latest model's cost: "+model.latestModel().computeCost(rdd))
      println("latest model's centers: ")
      model.latestModel().clusterCenters.foreach(v=>println("C: "+v.toString))

    })

    //start the loop
    ssc.start()
    ssc.awaitTermination()
  }
//give a key index for every n = coresetSize incoming examples
  def transformInput(rdd:RDD[Example],ssc:StreamingContext,coresetSize:Int):RDD[(Int,Example)] = {
    val acum = ssc.sparkContext.longAccumulator("cnt")
    rdd.map(ex=>{
      val cnt = Math.floor(acum.value / coresetSize).toInt
      acum.add(1)
      (cnt,ex)
    })
  }
  //merge examples with the same key into an array
  def init(rdd: RDD[(Int, Example)], coresetSize: Int): RDD[(Int, Array[Example])] = {
    //  println("Init started...")
    val combined = rdd.groupBy(_._1)
                      .mapValues(_.toArray)
                      .map(el => (el._1, el._2.map(lst => lst._2)))
    traverse(combined, coresetSize)
  }

 //group arrays for every two consecutive keys and merge them with coreset tree algorithm 
  def traverse(rdd: RDD[(Int, Array[Example])], coresetSize: Int): RDD[(Int, Array[Example])] = {
     if(rdd.count>1) {
       val result = rdd.groupBy { case (k, _) => k / 2 }
                       .map(x => (x._1, x._2.toArray))
       val ret = result.map(arr => {
         val newArr = arr._2.map(p => p._2)
         (arr._1, newArr.flatMap(line => line))
       })
       val retRDD = makeCoreset(ret, coresetSize)
       println("iterate")
       traverse(retRDD,coresetSize)
     }
     else rdd
  }

  def makeCoreset(rdd: RDD[(Int, Array[Example])], coresetSize: Int): RDD[(Int, Array[Example])] = {
    val coresetsRDD = rdd.map(cor => {
      val tree = new TreeCoreset
      val coreset = tree.retrieveCoreset(tree.buildCoresetTree(cor._2, coresetSize), new Array[Example](0))
      (cor._1, coreset)
    })
    
    
     /* def transformInput2(rdd:RDD[Example]):RDD[(Int,Example)] = {
    val partitioned = rdd.zipWithUniqueId().partitionBy(new HashPartitioner(10))

    val mapped =   partitioned.mapPartitionsWithIndex{
                        // 'index' represents the Partition No
                       // 'iterator' to iterate through all elements
                      //                         in the partition
      (index, iterator) => {
                            val myList = iterator.toList
                             // In a normal user case, we will do the
                             // the initialization(ex : initializing database)
                             // before iterating through each element
        myList.map(x => (index, x._1)).iterator
                           }
                     }
    mapped
  }
*/
    
    coresetsRDD
  }

}
