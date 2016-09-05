package ca.uwaterloo.cs.bigdata2016w.AynmShawn4.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

object SelectWhereOrderByQuery2 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Config(argv)

    log.info("Input: " + args.input())
    log.info("date: " + args.date())

    val conf = new SparkConf().setAppName("A5Q2")
    val sc = new SparkContext(conf)
    val shipdate = args.date().toString.split('-')

    val lineitem = sc.textFile(args.input()+"/lineitem.tbl")

    //var hashmapPart = HashMap[Int,String]()
    val part = sc.textFile(args.input()+"/part.tbl")
        .map(line => {
            val tokens = line.split('|')
          //  hashmapPart += (tokens(0).toInt -> tokens(1).toString)
            (tokens(0).toInt, tokens(1).toString)
            }).collectAsMap
    val broadcastPart = sc.broadcast(part)



    val supplier = sc.textFile(args.input()+"/supplier.tbl")
        .map(line => {
            val tokens = line.split('|')
            (tokens(0).toInt , tokens(1).toString)

            }).collectAsMap
    val broadcastSupplier = sc.broadcast(supplier)


    val outcome = lineitem
    	.map( line => {
            val tokens = line.split('|')
            (tokens(0).toInt, tokens(1).toInt, tokens(2).toInt, tokens(10)) 
            } )
    	.filter( 
    		l => {
                 val line = l._4.toString.split('-')

    			if (shipdate.length == 3){
    				(shipdate(0) == line(0)) && (shipdate(1) == line(1)) && (shipdate(2) == line(2))
    				
    			} else if (shipdate.length == 2){

    				(shipdate(0) == line(0)) && (shipdate(1) == line(1))
    			} else {

    				(shipdate(0) == line(0))
    			}
    			})
        .map(
            p => {
                if ((broadcastSupplier.value.contains(p._3)) && ((broadcastPart.value.contains(p._2)))) {
                    (p._1, (broadcastSupplier.value(p._3) , broadcastPart.value(p._2) ) )
                } else {
                    (-1, (0,0))
                }
                })
        .filter(
            p => {
                if (p._1 == -1){
                    false
                } else {
                    true
                }
                })
        .sortByKey()
        .take(20)
        .map(p => println("("+p._1 + "," +p._2._2 +"," + p._2._1 +")"))
	}

}
