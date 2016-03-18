package ca.uwaterloo.cs.bigdata2016w.AynmShawn4.assignment6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.math._

class Config2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, output, method)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "output model", required = true)
  val output = opt[String](descr = "output path", required = true)
  val method = opt[String](descr = "method", required = true)
}

object ApplyEnsembleSpamClassifier  {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Config2(argv)

    val conf = new SparkConf().setAppName("A6ENSEMBLE")
    val sc = new SparkContext(conf)


    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val model = sc.textFile(args.model()+"/part-00000").map{
    	line => {
    		val kv = line.replace("(", "").replace(")", "").split(",")
    		(kv(0).toInt, kv(1).toDouble)
	    		}}.collectAsMap
    val broadcastModel = sc.broadcast(model)

    val model1 = sc.textFile(args.model()+"/part-00001").map{
      line => {
        val kv = line.replace("(", "").replace(")", "").split(",")
        (kv(0).toInt, kv(1).toDouble)
          }}.collectAsMap
    val broadcastModel1 = sc.broadcast(model1)

    val model2 = sc.textFile(args.model()+"/part-00002").map{
      line => {
        val kv = line.replace("(", "").replace(")", "").split(",")
        (kv(0).toInt, kv(1).toDouble)
          }}.collectAsMap
    val broadcastModel2 = sc.broadcast(model2)

    var method = 0;
    if (args.method().equals("average")) {
      method = 1
    } else if (args.method().equals("vote")){
      method = 2
    }

    val textFile = sc.textFile(args.input()).map(
    	line => {
    		val featureArray = line.split(" ")
    		val docid = featureArray(0).toString
    		val hamOrSpam = featureArray(1).toString
        var score = 0d;
        var score1 = 0d;
        var score2 = 0d;

    		featureArray.drop(2).foreach(
    			p => {
            val temp = if (broadcastModel.value.contains(p.toInt)) broadcastModel.value(p.toInt) else 0
            val temp1 = if (broadcastModel1.value.contains(p.toInt)) broadcastModel1.value(p.toInt) else 0
            val temp2 = if (broadcastModel2.value.contains(p.toInt)) broadcastModel2.value(p.toInt) else 0
            
    				score += temp
            score1 += temp1
            score2 += temp2
    			}
        )

        var prediction = ""
        var average = 0d;
        if (method == 1){
          average = (score1 + score2 + score ) / 3
    		  prediction = if (average > 0) "spam" else "ham"
        } else if (method == 2){
          var s = 0
          var h = 0
          var k = 0
          var k1 = 0
          var k2 = 0
          if (score > 0) {
            k = 1
            s += 1;
          } else {
            k = 0 //1 is spam 0 is ham
            h += 1;
          }
           if (score1 > 0) {
            k1 = 1
            s += 1;
          } else {
            k1 = 0 //1 is spam 0 is ham
            h += 1;
          }
           if (score2 > 0) {
            k2 = 1
            s += 1;
          } else {
            k2 = 0 //1 is spam 0 is ham
            h += 1;
          }

          prediction = if ((k + k1 + k2) < 2) "spam" else "ham"
          average = s - h 
        }
    		(docid, hamOrSpam, average, prediction)
    	}
    ).saveAsTextFile(args.output())	

  }
}