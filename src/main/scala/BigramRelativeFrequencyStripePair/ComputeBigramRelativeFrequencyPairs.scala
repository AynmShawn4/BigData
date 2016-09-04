package ca.uwaterloo.cs.bigdata2016w.AynmShawn4.assignment2

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner

class myPartitioner(partitionsNum: Int) extends Partitioner {
 override def numPartitions: Int = partitionsNum
 override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[String]
        // `k` is assumed to go continuously from 0 to elements-1.

    return ( (k.split(", ").head.hashCode() & Integer.MAX_VALUE ) % numPartitions).toInt

  }
}


class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
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
        List( 
        (if (tokens.length > 1) tokens.sliding(2).map(p => p.mkString(", ")) else List()),
        (if (tk.length > 1)  tk.map(p => p + ", *" ).toList.dropRight(1) else List()   )
        ).flatten
      }) 
      .map(bigram => (bigram, 1))
      .reduceByKey(_ + _)
      .sortByKey()

      .partitionBy(new myPartitioner(args.reducers() ))

      .mapPartitions(pp => { var marginal = 0.0f
          pp.map(p => {

        if (p._1.split(", ")(1) == "*"){
          marginal = p._2.toFloat
          ("(" + p._1 +")", p._2 )
        } else {
          ("(" + p._1 +")", p._2 / marginal)
        }
        }
        )
        })
      counts.saveAsTextFile(args.output())
  }
}
