
Sys.setenv(SPARK_HOME="C:\\spark-2.1.1-bin-hadoop2.7")

.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))

library("SparkR", lib.loc="C:\\spark-2.1.1-bin-hadoop2.7\\R\\lib") # The use of \\ is for windows environment.

library(SparkR)

sc=sparkR.init(master="local")