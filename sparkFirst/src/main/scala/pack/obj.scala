package pack
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import sys.process._

object obj {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val spark= SparkSession.builder().getOrCreate()
					val df = spark.read.format("csv").option("header","true").load("file:///e:/data/text.txt")
					df.show()


	}
}