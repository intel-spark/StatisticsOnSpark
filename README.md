# Spark.statistics

Assembly of fundamental statistics implemented based on Apache Spark

## Requirements

This documentation is for Spark 1.3+. Other version will probably work yet not tested.

## Features

`Spark.statistics` intends to provide fundamental statistics functions.

Currently we support:
* One Sample T Test,
* Independent Samples T Test
* Paired Samples T Test
* One way ANOVA

Hopefully more features will come in quickly, next on the list:
* Post Hoc comparison
* Log likelihood
* Kolmogorov-Smirnov


## Example

### Scala API

```scala
    val sample1 = Array(100d, 200d, 300d, 400d)
    val sample2 = Array(101d, 205d, 300d, 400d)

    val rdd1 = sc.parallelize(sample1)
    val rdd2 = sc.parallelize(sample2)

    new TwoSampleIndependentTTest().tTest(rdd1, rdd2, 0.05))
    new TwoSampleIndependentTTest().tTest(rdd1, rdd2)
```
