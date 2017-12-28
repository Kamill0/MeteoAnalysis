produceHourlyResults <- function(dat, procFile, destDir) {
  #dat = read.csv(file=procFile, header = FALSE)
  #colnames(dat) = c("time", "timeDiscarded", "value")  
  
  outputDataFrame = setNames(data.frame(matrix(ncol = 2, nrow = 0)), c("time", "value"))
  
  
  hourlyDir = paste(destDir, "hourlyR", sep = "\\")
  createdDir = ifelse(!dir.exists(hourlyDir), dir.create(hourlyDir), FALSE)
  outFileName = paste("h", sapply(strsplit(procFile,split='\\',fixed=TRUE), tail, 1), sep="_")
  outCsvPath = paste(hourlyDir, outFileName, sep = "\\")
  
  # we are reading the first line from the file in order to set some variables:
  firstRow = dat[1,]
  #dt1 = as.POSIXct(dat$time[1],format="%Y-%m-%d %H:%M:%S",tz=Sys.timezone(),origin="1970-01-01 00:00.00 UTC")
  dt1 = as.POSIXct(dat$time[1],tz=Sys.timezone(),origin="1970-01-01")
  dt1Test = dat$time[1]
  print(paste("DT1ASPOSIXCT", dt1))
  print(paste("DT1normal", dt1Test))
  curHour = format(dt1, "%H")
  curDay = format(dt1, "%d")
  print(curHour)
  print(paste("TYPEOF CUR HOUR", typeof(curHour)))
  sum = 0
  counter = 0
  prevVal = 0

  # a = NA
  # if (a) {
  #   lala
  # }
  
  for (i in 2:nrow(dat)){
    dt = as.POSIXct(dat$time[i],tz=Sys.timezone(),origin="1970-01-01")
    dtHour = format(dt, "%H")
    if (dtHour == curHour) {
      val = as.numeric(dat$value[i])
      sum = sum + ifelse(!is.na(val), val, prevVal)
      counter = counter + 1
    } else {
      newDtDateStr = paste(format(dt, "%Y"), format(dt, "%m"), curDay, sep = "-")
      newDtTimeStr = paste(format(dt, "%H"), "00", "00", sep = ":")
      newDt = as.POSIXct(paste(newDtDateStr, newDtTimeStr), format="%Y-%m-%d %H:%M:%S",tz=Sys.timezone())

      curHour = format(dt, "%H")
      curDay = format(dt, "%d")

      if (counter == 0) {
        outputDataFrame <- rbind(outputDataFrame, data.frame("time" = newDt, "value" = 0))
      } else {
        val = as.numeric(format((as.numeric(sum) / as.numeric(counter)), digits=1, nsmall=1))

        outputDataFrame <- rbind(outputDataFrame, data.frame("time" = newDt, "value" = val))
      }

      sum = as.numeric(dat$value[i])
      sum = ifelse(!is.na(sum), sum, prevVal)
      counter = 1

      #print(newDt)

    }
    tmp = as.numeric(dat$value[i])
    if (!is.na(tmp)) {  # prevVal will store the last value that could have been converted to float, nothing else we can do here
      prevVal = tmp
    }
  }
  
  result = outputDataFrame
  #write.csv(result, file = outCsvPath, row.names=FALSE, quote = FALSE)
  return(result)
}

start.time <- Sys.time()

Sys.setenv(SPARK_HOME="C:\\spark-2.1.1-bin-hadoop2.7")

.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))

library("SparkR", lib.loc="C:\\spark-2.1.1-bin-hadoop2.7\\R\\lib") # The use of \\ is for windows environment.

library(SparkR)

sc=sparkR.session(master="local", spark.executor.memory = "4g")

procFile = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\Data\\s018\\winds\\s018_winds_2017-07-01_2017-07-31.csv"

#dat = read.csv(file=procFile, header = FALSE)
dat = read.df(procFile, "csv", header = "false", inferSchema = "true", na.strings = "NA")
colnames(dat) = c("time", "timeDiscarded", "value") 

#randomVector = matrix(base::sample(1:4, 10000, replace=TRUE), nrow = length(dat$time), ncol = 1)

#testDat = data.frame(dat, randomVector)
#colnames(testDat) = c("time", "timeDiscarded", "value", "partition")


#df <- as.DataFrame(dat)

#head(select(dat, dat$time, spark_partition_id()))

head(summarize(groupBy(dat, spark_partition_id()), count = n(spark_partition_id())))

newDf <- repartition(dat, 4L)

head(summarize(groupBy(newDf, spark_partition_id()), count = n(spark_partition_id())))

head(filter(newDf, spark_partition_id() == 0))

#schema <- structType(structField("time", "double"), structField("value", "double"))

#ldf3 <- dapplyCollect(newDf, function(x) { x <- produceHourlyResults(x, procFile, subSubDir) })
#head(ldf2, 3)


end.time <- Sys.time()
time.taken <- end.time - start.time
time.taken


#result <- produceHourlyResults(filePath, subSubDir)