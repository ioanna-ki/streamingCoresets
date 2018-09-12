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
    val testPath = args(4).toString

    val trainExamples = ssc.textFileStream(trainPath).map(arr => {
      val tokens = arr.split(",")
      new Example(new DenseInstance(tokens.map(_.toDouble)))
    })

    val testExamples =ssc.textFileStream(testPath)
       .map(arr => {
         val coord: String = "("+Random.nextInt(kOption)+",["+arr+"])"
             coord
       }).map(LabeledPoint.parse)

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
    val coresetVector = coreset.map(c=>c._2)
      .flatMap(line => line)
      .map(e => {
        val p = "[" + e.in.toString + "]"
        p
    }).map(Vectors.parse)

    val model = new StreamingKMeans()
          .setK(kOption)
          .setDecayFactor(1.0)
          .setRandomCenters(dim, 0.0)
   model.trainOn(coresetVector)

    val out = testExamples.map(lp=>(lp.label,model.latestModel.predict(lp.features)))
    model.predictOnValues(testExamples.map(lp => (lp.label, lp.features))) foreachRDD { (rdd, time) =>
      println(s"++++\ntime: $time")
      val lm = model.latestModel
      lm.clusterCenters.foreach(v=>println(v.toString))
      rdd.keyBy(_._2).groupByKey.map{
        case (key, iter) => (key, iter.map(_._1))
      } foreach println
    }

    //start the loop
    ssc.start()
    ssc.awaitTermination()
  }
  def transformInput2(rdd:RDD[Example]):RDD[(Int,Example)] = {
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

  def transformInput(rdd:RDD[Example],ssc:StreamingContext,coresetSize:Int):RDD[(Int,Example)] = {
    val acum = ssc.sparkContext.longAccumulator("cnt")
    rdd.map(ex=>{
      val cnt = Math.floor(acum.value / coresetSize).toInt
      acum.add(1)
      (cnt,ex)
    })
  }



  def init(rdd: RDD[(Int, Example)], coresetSize: Int): RDD[(Int, Array[Example])] = {
    //  println("Init started...")
    val combined = rdd.groupBy(_._1)
                      .mapValues(_.toArray)
                      .map(el => (el._1, el._2.map(lst => lst._2)))
    traverse(combined, coresetSize)
  }

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
    coresetsRDD
  }

}
