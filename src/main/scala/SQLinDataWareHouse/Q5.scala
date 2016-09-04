package ca.uwaterloo.cs.bigdata2016w.AynmShawn4.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Config1(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input)
  val input = opt[String](descr = "input path", required = true)
}

object Q5  {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Config1(argv)

    log.info("Input: " + args.input())

    val conf = new SparkConf().setAppName("A5Q5")
    val sc = new SparkContext(conf)


    val lineitem = sc.textFile(args.input()+"/lineitem.tbl")

    val nation = sc.textFile(args.input()+"/nation.tbl")
        .map(line => {
            val tokens = line.split('|')
            (tokens(0).toInt, tokens(1).toString)
            }).collectAsMap
    val broadcastNation = sc.broadcast(nation)


    val customer = sc.textFile(args.input()+"/customer.tbl")
        .map(line => {
            val tokens = line.split('|')
            (tokens(0).toInt , tokens(3).toInt)

            }).collectAsMap
    val broadcastCustomer = sc.broadcast(customer)

    val orders = sc.textFile(args.input()+"/orders.tbl")

    val outcome = lineitem
    	.map( line => {
            val tokens = line.split('|')
            (tokens(0).toInt, tokens(10).substring(0,7)) 
            } )
        .cogroup(
            orders.map( line => (line.split('|')(0).toInt, line.split('|')(1).toInt ))
             .map(
                p => {
                    val cp = broadcastCustomer.value
                    (p._1, cp(p._2))
                    }
                )
             .filter(
                p => (
                    if ((p._2 == 3) || (p._2 == 24)) {
                        true
                    } else {
                        false
                    }
                    ) 
                )
            )
        .filter(
            p =>
            ( if (p._2._2.toList.length == 0){
                false
            } else {
                true
            }
                ))
        .flatMap(p => {
            var temp = p._2._1.toList
            for (i <- 0 to (temp.length - 1) ) yield (( temp(i), p._2._2.toList(0)), 1)
        })
        .reduceByKey(_ + _)
        .sortBy(p => (p._1))
        .collect()

        println("CANADA")
        outcome
        .foreach(p =>  {
            if (p._1._2.toInt == 3){
                println("("+p._1._1 + "," + p._2 + ")")
            } 
            })
        println("USA")
        outcome
        .foreach(p =>  {
         
            if (p._1._2.toInt == 24){
                println("("+p._1._1 + "," + p._2 + ")")
            }
            })

	}

}
