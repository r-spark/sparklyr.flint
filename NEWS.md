# sparklyr.flint 0.2.2

- Edgar Ruiz (https://github.com/edgararuiz) will be the new maintainer of
  {sparklyr.flint} moving forward.

# sparklyr.flint 0.2.1

- Migrated Maven artifact repo for the customized builds of the Flint time
  series library used by {sparklyr.flint} from bintray to github

# sparklyr.flint 0.2.0

- Implemented R interface for Flint ASOF join functionalities

- Added support for more summarizers (e.g., skewness, kurtosis, OLS regression,
  EWMA, etc)

- Some minor improvements in documentation

- Implemented `to_sdf()` function for retrieving data from a TimeSeriesRDD to a
  Spark dataframe

- Implemented `spark_dataframe.ts_rdd()` function for retrieving the underlying
  Spark dataframe Java object contained in a TimeSeriesRDD

# sparklyr.flint 0.1.1

- Replaced '\dontrun' in examples with something more reasonable

# sparklyr.flint 0.1.0

- First submission of `sparklyr.flint` to CRAN, featuring R interfaces to commonly
  used summarizers in Flint
