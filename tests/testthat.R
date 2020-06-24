library(sparklyr)
library(sparklyr.flint)
library(testthat)

if (identical(Sys.getenv("NOT_CRAN"), "true")) {
  on.exit({ spark_disconnect_all() })

  filter <- Sys.getenv("TESTTHAT_FILTER", unset = "")
  if (identical(filter, "")) filter <- NULL

  reporter <- MultiReporter$new(reporters = list(
    ProgressReporter$new(),
    CheckReporter$new(),
    SummaryReporter$new()
  ))
  test_check("sparklyr.flint", filter = filter, reporter = reporter)
}
