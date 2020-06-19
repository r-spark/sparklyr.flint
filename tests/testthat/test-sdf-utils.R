context("sdf-utils")

sc <- testthat_spark_connection()
sorted_ts_rdd_builder <- ts_rdd_builder(sc, is_sorted = FALSE)

test_that("ts_rdd_builder can process sorted data frame", {
  sdf <- testthat_sorted_sdf()
  ts_rdd <- sorted_ts_rdd_builder$fromDF(sdf)

  # TODO:
  succeed()
})

test_that("ts_rdd_builder can work with RDD+schema", {
  sdf <- testthat_sorted_sdf()
  rdd <- invoke(spark_dataframe(sdf), "rdd")
  schema <- invoke(spark_dataframe(sdf), "schema")
  ts_rdd <- sorted_ts_rdd_builder$fromRDD(rdd, schema)

  # TODO:
  succeed()
})
