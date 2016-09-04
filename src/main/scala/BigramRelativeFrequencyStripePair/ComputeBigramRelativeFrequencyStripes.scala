package ca.uwaterloo.cs.bigdata2016w.AynmShawn4.assignment2

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner


class myPartitioner2(partitionsNum: Int) extends Partitioner {
 override def numPartitions: Int = partitionsNum
 override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[String]
        // `k` is assumed to go continuously from 0 to elements-1.

    return ( (k.hashCode() & Integer.MAX_VALUE ) % numPartitions).toInt

  }
}

/*
class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
}*/

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Count")
    val sc = new SparkContext(conf)


    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val counts = textFile

      .flatMap(line => {

        val tokens = tokenize(line)
        val tk = tokenize(line)

        if (tokens.length > 1) tokens.sliding(2).toList.map(p => {
          val map = scala.collection.immutable.Map[String,Int]()

          val b = map + (p(1) -> 1)

          (p(0), b ) }) else List()
   
      }).reduceByKey((x,y ) => x ++ y.map{ case (k,v) => k -> (v + x.getOrElse(k,0)) }  )
      .sortByKey()

      .partitionBy(new myPartitioner2(args.reducers() ))

      .mapPartitions(pp => { 
          pp.map(p => {
            var sum = 0.0f
            val mp =  scala.collection.mutable.Map(p._2.toSeq: _*) 

            mp.foreach {case(key, value) => {sum = sum + value}}
            var ret =  mp.map {case(key, value) =>  
              (key, value / sum )
            }

            (p._1, ret)

          }
        )
      })
      counts.saveAsTextFile(args.output())
  }
}
