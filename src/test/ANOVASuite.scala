package test

import java.util

import main.ANOVA.OneWayANOVA
import org.apache.commons.math3.stat.inference.TestUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.stat.OneSampleTTest
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by yuhao on 1/4/16.
  */
object ANOVASuite {

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  val conf = new SparkConf().setAppName("TallSkinnySVD").setMaster("local")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    OneWayANOVA
  }

  def OneWayANOVA(): Unit ={
    val sample1 = Array(100d, 200d, 300d, 400d)
    val sample2 = Array(101d, 200d, 300d, 400d)
    val sample3 = Array(102d, 200d, 300d, 400d)
    val data = new util.ArrayList[Array[Double]]()
    data.add(sample1)
    data.add(sample2)
    data.add(sample3)

    val rdd1 = sc.parallelize(sample1)
    val rdd2 = sc.parallelize(sample2)
    val rdd3 = sc.parallelize(sample3)
    val rddData = Seq(rdd1, rdd2, rdd3)

    assert(TestUtils.oneWayAnovaFValue(data) == new OneWayANOVA().anovaFValue(rddData))
    assert(TestUtils.oneWayAnovaPValue(data) == new OneWayANOVA().anovaPValue(rddData))
  }

}
