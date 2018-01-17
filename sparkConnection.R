produceHourlyResults <- function(dat) {
  outputDataFrame = setNames(data.frame(matrix(ncol = 2, nrow = 0)), c("time", "value"))
  
  #hourlyDir = paste(destDir, "hourlyR", sep = "\\")
  #createdDir = ifelse(!dir.exists(hourlyDir), dir.create(hourlyDir), FALSE)
  #outFileName = paste("h", sapply(strsplit(procFile,split='\\',fixed=TRUE), tail, 1), sep="_")
  #outCsvPath = paste(hourlyDir, outFileName, sep = "\\")
  
  # we are reading the first line from the file in order to set some variables:
  firstRow = dat[1,]
  dt1 = as.POSIXct(dat$time[1],format="%Y-%m-%d %H:%M:%S",tz=Sys.timezone(),origin="1970-01-01 00:00.00 UTC")
  #dt1 = as.POSIXct(dat$time[1],tz=Sys.timezone(),origin="1970-01-01")
  
  curHour = format(dt1, "%H")
  curDay = format(dt1, "%d")
  sum = 0
  counter = 0
  prevVal = 0

  
  for (i in 2:nrow(dat)){
    dt = as.POSIXct(dat$time[i],tz=Sys.timezone(),format="%Y-%m-%d %H:%M:%S",origin="1970-01-01")
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

# This procedure takes a list of spark data frames, combines them into one (as separate partitions) and feeds 
# them to the spark cluster for processing. Afterwards, the result is split and stored into CSV files
executeAndWrite <- function(dat, destDir) {
  partitionNo = length(dat)
  inputDat = list()
  
  for (i in 1:partitionNo){
    #print(typeof(dat[i]))
    tmpDat = read.csv(file=dat[[i]], header = FALSE)
    colnames(tmpDat) = c("time", "timeDiscarded", "value")
    partitionCol = matrix(i, nrow = length(tmpDat$time), ncol = 1)
    
    singlePartitionDat = data.frame(tmpDat, partitionCol)
    colnames(singlePartitionDat) = c("time", "timeDiscarded", "value", "partition")
    
    inputDat = rbind(inputDat, singlePartitionDat)
  }
  
  df <- as.DataFrame(inputDat) # represent as Spark data frame
  repartitionedDf <- repartition(df, partitionNo, col = df$partition)
  
  processedDf <- dapplyCollect(repartitionedDf, function(x) { x <- produceHourlyResults(x) })

}

initSparkSession <- function(driverMemory) {
  Sys.setenv(SPARK_HOME="C:\\spark-2.1.1-bin-hadoop2.7")
  
  .libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
  
  library("SparkR", lib.loc="C:\\spark-2.1.1-bin-hadoop2.7\\R\\lib") # The use of \\ is for windows environment
  library(SparkR)
  sparkR.session(master="local[*]",  sparkConfig = list(spark.driver.memory = driverMemory))
}

start.time <- Sys.time()

numOfCores = 4 # This is environment dependant, should probably be stored somewhere (a prop file?)
initSparkSession("4g")

dataDir = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\Data"
stations = list.files(dataDir)
for(station in stations){
  subDir = paste(dataDir,station,sep = "\\")
  typeOfMeasurment = list.files(subDir)
  for(measurement in typeOfMeasurment){
    subSubDir = paste(subDir,measurement,sep = "\\")
    months = list.files(subSubDir, include.dirs = FALSE)
    dfList <- list()
    for(month in months){
      filePath = paste(subSubDir,month,sep="\\")
      if (file_test("-f", filePath)) {
        #print(paste("Processing file:", month))
        len = length(dfList)
        if (len < numOfCores) {
          dfList[[len + 1]] <- filePath
        } else {
          # This means we have reached the parallel capacity and can now proceed with our batch
          print("Processing batch no: TODO")
          executeAndWrite(dfList, subSubDir)
          dfList <- list(filePath)
        }
      } else {
        
        print("Not a regular file, skipping")
      }
    }
    
    if (length(dfList) != 0) {# We have to send the last batch to spark (in case there is still something not processed)
      print("Processing remaining batch: ")
      executeAndWrite(dfList, subSubDir)
    }
    
  }
}




procFile = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\Data\\s018\\winds\\s018_winds_2017-07-01_2017-07-31.csv"
procFile2 = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\Data\\s018\\winds\\s018_winds_2017-06-01_2017-06-30.csv"
procFile3 = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\Data\\s018\\winds\\s018_winds_2017-05-01_2017-05-31.csv"


dat = read.csv(file=procFile, header = FALSE)
dat2 = read.csv(file=procFile2, header = FALSE)
dat3 = read.csv(file=procFile3, header = FALSE)

#dat = read.df(procFile, "csv", header = "false", inferSchema = "true", na.strings = "NA")
colnames(dat) = c("time", "timeDiscarded", "value") 
colnames(dat2) = c("time", "timeDiscarded", "value")
colnames(dat3) = c("time", "timeDiscarded", "value")

randomVector0 = matrix(0, nrow = length(dat$time), ncol = 1)
randomVector1 = matrix(1, nrow = length(dat2$time), ncol = 1)
randomVector2 = matrix(2, nrow = length(dat3$time), ncol = 1)


testDat = data.frame(dat, randomVector0)
testDat2 = data.frame(dat2, randomVector1)
testDat3 = data.frame(dat3, randomVector2)


#testDat = data.frame(dat, randomVector)
colnames(testDat) = c("time", "timeDiscarded", "value", "partition")
colnames(testDat2) = c("time", "timeDiscarded", "value", "partition")
colnames(testDat3) = c("time", "timeDiscarded", "value", "partition")

testDatMerged = rbind(testDat, testDat2, testDat3)

df <- as.DataFrame(testDatMerged)

#head(select(dat, dat$time, spark_partition_id()))

#head(summarize(groupBy(df, spark_partition_id()), count = n(spark_partition_id())))

newDf <- repartition(df, 3L, col = df$partition)

#head(summarize(groupBy(newDf, spark_partition_id()), count = n(spark_partition_id())))

#head(filter(newDf, spark_partition_id() == 0))

#schema <- structType(structField("time", "double"), structField("value", "double"))

ldf3 <- dapplyCollect(newDf, function(x) { x <- produceHourlyResults(x, procFile, subSubDir) })
#head(ldf3, 3)


end.time <- Sys.time()
time.taken <- end.time - start.time
time.taken


#result <- produceHourlyResults(filePath, subSubDir)