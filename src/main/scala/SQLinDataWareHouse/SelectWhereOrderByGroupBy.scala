package ca.uwaterloo.cs.bigdata2016w.AynmShawn4.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

object Q4  {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Config(argv)

    log.info("Input: " + args.input())
    log.info("date: " + args.date())

    val conf = new SparkConf().setAppName("A5Q2")
    val sc = new SparkContext(conf)
    val shipdate = args.date().toString.split('-')


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
            (tokens(0).toInt, tokens(10)) 
            } )
    	.filter( 
    		l => {
                 val line = l._2.toString.split('-')

    			if (shipdate.length == 3){
    				(shipdate(0) == line(0)) && (shipdate(1) == line(1)) && (shipdate(2) == line(2))
    				
    			} else if (shipdate.length == 2){

    				(shipdate(0) == line(0)) && (shipdate(1) == line(1))
    			} else {

    				(shipdate(0) == line(0))
    			}
    			})
        .cogroup(
            orders.map( line => (line.split('|')(0).toInt, line.split('|')(1).toInt ))
             .map(
                p => {
                    val cp = broadcastCustomer.value
                    (p._1, cp(p._2))
                    }
                )
            )
        .filter(
            p => {
                if (p._2._1.toList.length == 0){
                    false
                } else {
                    true
                }
                })
        .flatMap(p => {
            var temp = p._2._1.toList
            for (i <- 0 to (temp.length - 1) ) yield (p._1, (temp(i), p._2._2))
        })
        .map(
            p => {
                val np = broadcastNation.value
                ((p._2._2.toList(0), np(p._2._2.toList(0))), 1)
                })
        .reduceByKey(_ + _)
        .sortByKey()
        .collect()
        .foreach(p => println("("+p._1._1 + "," + p._1._2 + ","+ p._2 + ")"))
	}

}
