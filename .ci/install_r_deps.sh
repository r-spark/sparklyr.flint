#!/bin/bash

set -efux -o pipefail

R_REMOTES_NO_ERRORS_FROM_WARNINGS=true Rscript - <<_RSCRIPT_EOF_
  if (!require(devtools)) install.packages("devtools")
  devtools::install_deps(dependencies = c("Imports"))
_RSCRIPT_EOF_
