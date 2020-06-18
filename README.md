# sparklyr.flint: Sparklyr extension for Flint

Sparklyr.flint is a sparklyr extension making Flint time series library functionalities (https://github.com/twosigma/flint) accessible through R.

This extension is currently under active development. It requires the master branch of sparklyr (i.e., the version from `devtools::install_github("sparklyr/sparklyr", ref = "master")` which is newer that sparklyr 1.2) to be able to load the `com.twosigma:sparklyr-flint_*` artifacts correctly from the `https://dl.bintray.com/yl790/maven` repository, along with all transitive dependencies from Maven central.

The `com.twosigma:sparklyr-flint_*` artifacts contain minor modifications (mostly changes to the `build.sbt` file) needed to ensure Flint time series functionalities work with Spark 2.4 and Spark 3.0. Artifact names and locations are subject to change. They most likely will be moved to Maven central in future, possibly under a different group ID as well (TBD).

At the moment, Flint time series functionalities are accessible through both Spark 2.x and Spark 3.0 via sparklyr, but a nice declarative R interface for such functionalities is still missing. Here is an example illustrating what that means:

``` r
library(sparklyr)
library(sparklyr.flint)

spark_version <- "2.4"   # <- This extension now works for both Spark 2.x and Spark 3.x
# spark_version <- "3.0.0-preview2"
sc <- spark_connect(master = "local", version = spark_version)

# download some test data
data_file <- "/tmp/sp500.csv"
download.file("https://raw.githubusercontent.com/johnashu/datacamp/master/sp500.csv", data_file)
# create a time column of type timestamp to make data useable by Flint
sdf <- spark_read_csv(sc, path = data_file) %>%
  dplyr::mutate(time = to_timestamp(Date))

# we should wrap those details in some R methods
time_unit <- invoke_static(sc, "java.util.concurrent.TimeUnit", "SECONDS")
builder <- invoke_new(sc, "com.twosigma.flint.timeseries.TimeSeriesRDDBuilder", TRUE, time_unit, "time")

ts_rdd <- invoke(builder, "fromDF", spark_dataframe(sdf))
print(ts_rdd)

# and those as well
window <- invoke_static(sc, "com.twosigma.flint.timeseries.Windows", "pastAbsoluteTime", "7day")
summarizer <- invoke_static(sc, "com.twosigma.flint.timeseries.Summarizers", "ewma", "Open", 0.05, "time", "1d", "legacy")

summary <- invoke(ts_rdd, "summarizeWindows", window, summarizer, list())

print(summary)

summary_sdf <- invoke(summary, "toDF") %>% sdf_register()

print(summary_sdf %>% collect())

# ^^ As you can see, at the moment we need many `sparklyr::invoke*` calls to accomplish something simple.
# Ideally with a nicer R interface fully developed, 0 `sparklyr::invoke*` call will be needed.
```
