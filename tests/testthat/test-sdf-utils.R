context("sdf-utils")

sc <- testthat_spark_connection()
sorted_ts_rdd_builder <- ts_rdd_builder(sc, is_sorted = TRUE)
unsorted_ts_rdd_builder <- ts_rdd_builder(sc, is_sorted = FALSE)
date_col_ts_rdd_builder <- ts_rdd_builder(sc, is_sorted = TRUE, time_column = "date")

verify_result <- function(df) {
  expect_equivalent(
    df,
    tibble::tibble(
      time = as.Date(seq(6), origin = as.Date("1970-01-01")),
      value <- c(1, 4, 2, 8, 5, 7)
    )
  )
}

test_that("ts_rdd_builder can process sorted data frame", {
  sdf <- testthat_sorted_sdf()
  ts_rdd <- sorted_ts_rdd_builder$fromDF(sdf)

  verify_result(ts_rdd %>% collect())
})

test_that("ts_rdd_builder can process unsorted data frame", {
  sdf <- testthat_unsorted_sdf()
  ts_rdd <- unsorted_ts_rdd_builder$fromDF(sdf)

  verify_result(ts_rdd %>% collect())
})

test_that("ts_rdd_builder can work with RDD+schema", {
  sdf <- testthat_sorted_sdf()
  rdd <- invoke(spark_dataframe(sdf), "rdd")
  schema <- invoke(spark_dataframe(sdf), "schema")
  ts_rdd <- sorted_ts_rdd_builder$fromRDD(rdd, schema)

  verify_result(ts_rdd %>% collect())
})

test_that("ts_rdd_builder can work with time column of type Date", {
  sdf <- testthat_date_sdf()
  ts_rdd <- date_col_ts_rdd_builder$fromDF(sdf)

  verify_result(ts_rdd %>% collect())
})