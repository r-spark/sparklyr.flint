context("sdf-utils")

sc <- testthat_spark_connection()

test_that("ts_rdd_builder can process sorted data frame", {
  sdf <- testthat_sorted_sdf()
  builder <- ts_rdd_builder(sc, is_sorted = FALSE)
  ts_rdd <- builder$fromDF(sdf)

  # TODO:
  succeed()
})
