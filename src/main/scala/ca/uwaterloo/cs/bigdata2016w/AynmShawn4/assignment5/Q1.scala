package ca.uwaterloo.cs.bigdata2016w.AynmShawn4.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._


class Config(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date of shipment", required = true)
}

object Q1  {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Config(argv)

    log.info("Input: " + args.input())
    log.info("date: " + args.date())

    val conf = new SparkConf().setAppName("A5Q1")
    val sc = new SparkContext(conf)
    val shipdate = args.date().toString.split('-')

    val textFile = sc.textFile(args.input()+"/lineitem.tbl")
    val counts = textFile
    	.map( line => line.split('|')(10).toString.split('-'))
    	.filter( 
    		line => (
    			if (shipdate.length == 3){
    				(shipdate(0) == line(0)) && (shipdate(1) == line(1)) && (shipdate(2) == line(2))
    				
    			} else if (shipdate.length == 2){
    				(shipdate(0) == line(0)) && (shipdate(1) == line(1))
    			} else {
    				(shipdate(0) == line(0))
    			}
    			 ) ).count()
    	println("ANSWER=" + counts)
    }
}