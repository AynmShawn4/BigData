package ca.uwaterloo.cs.bigdata2016w.AynmShawn4.assignment6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.math._

class Config1(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, output)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "output model", required = true)
  val output = opt[String](descr = "output path", required = true)
}

object ApplySpamClassifier  {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Config1(argv)

    val conf = new SparkConf().setAppName("A6APPLY")
    val sc = new SparkContext(conf)


    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val model = sc.textFile(args.model()+"/part-00000").map{
    	line => {
    		val kv = line.replace("(", "").replace(")", "").split(",")
    		(kv(0).toInt, kv(1).toDouble)
	    		}}.collectAsMap
    val broadcastModel = sc.broadcast(model)


    val textFile = sc.textFile(args.input()).map(
    	line => {
    		val featureArray = line.split(" ")
    		val docid = featureArray(0).toString
    		val hamOrSpam = featureArray(1).toString
        var score = 0d;
    		featureArray.drop(2).foreach(
    			p => {
    				val temp = if (broadcastModel.value.contains(p.toInt)) broadcastModel.value(p.toInt) else 0
    				score += temp
    				})
    		val prediction = if (score > 0) "spam" else "ham"
    		(docid, hamOrSpam, score, prediction)
    	}
    ).saveAsTextFile(args.output())	

  }
}