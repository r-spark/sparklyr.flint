context("sdf-utils")

sc <- testthat_spark_connection()
sorted_ts_rdd_builder <- ts_rdd_builder(sc, is_sorted = FALSE)
date_col_ts_rdd_builder <- ts_rdd_builder(sc, is_sorted = FALSE, time_column = "date")

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

test_that("ts_rdd_builder can work with time column of type Date", {
  sdf <- testthat_date_sdf()
  ts_rdd <- date_col_ts_rdd_builder$fromDF(sdf)

  ts_sdf <- invoke(ts_rdd, "toDF") %>% sdf_register()
  # TODO:
  print(ts_sdf %>% collect())

  # TODO:
  succeed()
})
