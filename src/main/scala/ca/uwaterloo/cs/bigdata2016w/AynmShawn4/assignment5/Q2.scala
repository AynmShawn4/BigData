package ca.uwaterloo.cs.bigdata2016w.AynmShawn4.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

object Q2  {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Config(argv)

    log.info("Input: " + args.input())
    log.info("date: " + args.date())

    val conf = new SparkConf().setAppName("A5Q2")
    val sc = new SparkContext(conf)
    val shipdate = args.date().toString.split('-')

    val lineitem = sc.textFile(args.input()+"/lineitem.tbl")
    val orders = sc.textFile(args.input()+"/orders.tbl")

    val outcome = lineitem
    	.map( line => (line.split('|')(0).toInt, line.split('|')(10)  ) )
    	.filter( 
    		l => (
    			if (shipdate.length == 3){
    				 var line = l._2.toString.split('-')

    				(shipdate(0) == line(0)) && (shipdate(1) == line(1)) && (shipdate(2) == line(2))
    				
    			} else if (shipdate.length == 2){
    				  var line = l._2.toString.split('-')

    				(shipdate(0) == line(0)) && (shipdate(1) == line(1))
    			} else {
    				  var line = l._2.toString.split('-')

    				(shipdate(0) == line(0))
    			}
    			 ) )
       	.cogroup(orders.map( line => (line.split('|')(0).toInt, line.split('|')(6) )))
    	.filter(
    		p => (
    			if (p._2._1.toList.length == 0) {
    				false
    			} else {
    				true
    			} 
    			)
    		)
    	.flatMap(p => {
    		var temp = p._2._1.toList
    		for (i <- 0 to (temp.length - 1) ) yield (p._1, (temp(i), p._2._2))
    	})
    	.sortByKey()
    	.take(20)
    	.map(p => println("("+p._2._2.toList(0) + "," +p._1+")"))


	}

}