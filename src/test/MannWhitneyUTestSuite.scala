package test

import org.apache.commons.math3.stat.inference.MannWhitneyUTest
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yuhao on 2/8/16.
 */
object MannWhitneyUTestSuite {

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  val conf = new SparkConf().setAppName("TallSkinnySVD").setMaster("local")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    testMannWhitneyU
    testMannWhitneyUTest
  }

  private def testMannWhitneyU(): Unit ={
    val sample1 = Array(1d, 3d, 5, 7)
    val sample2 = Array(2, 4, 6, 8d)

    val rdd1 = sc.parallelize(sample1)
    val rdd2 = sc.parallelize(sample2)

    val result = new MannWhitneyUTest()
      .mannWhitneyU(sample1, sample2)
    val result2 = org.apache.spark.mllib.stat.test.MannWhitneyUTest.mannWhitneyU(rdd1, rdd2)
    assert(result == result2)
  }

  private def testMannWhitneyUTest(): Unit ={
    val sample1 = Array(1d, 3d, 5, 7)
    val sample2 = Array(2, 4, 6, 8d)

    val rdd1 = sc.parallelize(sample1)
    val rdd2 = sc.parallelize(sample2)

    val result = new MannWhitneyUTest()
      .mannWhitneyUTest(sample1, sample2)
    val result2 = org.apache.spark.mllib.stat.test.MannWhitneyUTest.mannWhitneyUTest(rdd1, rdd2)
    println(result)
    println(result2)
    assert(result == result2)
  }



}
