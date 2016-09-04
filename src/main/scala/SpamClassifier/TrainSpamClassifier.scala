package ca.uwaterloo.cs.bigdata2016w.AynmShawn4.assignment6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.math._
import util.Random


class Config(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, shuffle)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "output model", required = true)
  val shuffle = toggle("shuffle")


}

object TrainSpamClassifier  {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Config(argv)

    val conf = new SparkConf().setAppName("A6")
    val sc = new SparkContext(conf)


    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
	// w is the weight vector (make sure the variable is within scope)
	val w = scala.collection.mutable.Map[Int, Double]()

	val delta = 0.002

	// Scores a document based on its list of features.
	def spamminess(features: Array[Int]) : Double = {
	  var score = 0d
	  features.foreach(f => if (w.contains(f)) score += w(f))
	  score
	}

	var textFile = sc.textFile(args.input())
	if (args.shuffle.isSupplied ){
		textFile = sc.textFile(args.input()).map(line => {
			(Random.nextInt, line)
		})
		.sortByKey()
		.map(line =>{
			line._2
			})
	}

	val trained = textFile.map(line =>{
	  // Parse input
	 
	  val featureArray = line.split(" ")
	  val isSpam = if (featureArray(1).equals("spam")) 1 else 0
	  val features = featureArray.drop(2).map(_.toInt)

	  (0, (featureArray(0), isSpam, features))
	  })
	  .groupByKey(1)
	  .flatMap(
	  	f => {
	  		f._2.foreach(
	  			p => {
	  				val isSpam = p._2  // label
					val features = p._3 // feature vector of the training instance

					// Update the weights as follows:
					val score = spamminess(features)
					val prob = 1.0 / (1 + exp(-score))
					features.foreach(f => {
					  if (w.contains(f)) {
					    w(f) += (isSpam - prob) * delta
					  } else {
					    w(f) = (isSpam - prob) * delta
					   }
					})

	  			})
	  			w
	  		}).saveAsTextFile(args.model())	  

  }
}
