package main.ANOVA

import org.apache.commons.math3.distribution.FDistribution
import org.apache.spark.rdd.RDD


/**
 * Created by yuhao on 1/1/16.
 */
class OneWayANOVA {

  /**
   * Performs an ANOVA test, evaluating the null hypothesis that there
   * is no difference among the means of the data categories.
   *
   * @param categoryData Collection of RDD[Double], each containing data for one category
   * @param alpha significance level of the test
   * @return true if the null hypothesis can be rejected with
   *         confidence 1 - alpha
   */
  def anovaTest(categoryData: Iterable[RDD[Double]], alpha: Double): Boolean = {
    anovaPValue(categoryData) < alpha
  }

  /**
   * Computes the ANOVA F-value for a collection of RDD[double].
   *
   * This implementation computes the F statistic using the definitional
   * formula
   * F = msbg/mswg
   * where
   * msbg = between group mean square
   * mswg = within group mean square
   *
   * @param categoryData Collection of RDD[Double], each containing data for one category
   * @return Fvalue
   */
  def anovaFValue(categoryData: Iterable[RDD[Double]]): Double = {
    getAnovaStats(categoryData).F
  }



  /**
   * Computes the ANOVA P-value for a collection of double[]
   * arrays.
   *
   * @param categoryData Collection of RDD[Double], each containing data for one category
   * @return Pvalue
   */
  def anovaPValue(categoryData: Iterable[RDD[Double]]): Double = {
    val anovaStats = getAnovaStats(categoryData)

    val fdist: FDistribution = new FDistribution(null, anovaStats.dfbg, anovaStats.dfwg)
    return 1.0 - fdist.cumulativeProbability(anovaStats.F)
  }

  private case class ANOVAStats(dfbg: Double, dfwg: Double, F: Double)

  private def getAnovaStats(categoryData: Iterable[RDD[Double]]): ANOVAStats = {
    var dfwg: Long = 0
    var sswg: Double = 0
    var totsum: Double = 0
    var totsumsq: Double = 0
    var totnum: Long = 0

    for (data <- categoryData) {
      val sum: Double = data.sum()
      val sumsq: Double = data.map(i => i * i).sum()
      val num = data.count()
      totnum += num
      totsum += sum
      totsumsq += sumsq
      dfwg += num - 1
      val ss: Double = sumsq - ((sum * sum) / num)
      sswg += ss
    }

    val sst: Double = totsumsq - ((totsum * totsum) / totnum)
    val ssbg: Double = sst - sswg
    val dfbg: Int = categoryData.size - 1
    val msbg: Double = ssbg / dfbg
    val mswg: Double = sswg / dfwg
    val F: Double = msbg / mswg
    ANOVAStats(dfbg, dfwg, F)
  }


}
