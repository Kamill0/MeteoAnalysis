produceHourlyResults <- function(dat) {
  outputDataFrame = setNames(data.frame(matrix(ncol = 3, nrow = 0), stringsAsFactors = FALSE), c("time", "value", "partition"))
  
  #hourlyDir = paste(destDir, "hourlyR", sep = "\\")
  #createdDir = ifelse(!dir.exists(hourlyDir), dir.create(hourlyDir), FALSE)
  #outFileName = paste("h", sapply(strsplit(procFile,split='\\',fixed=TRUE), tail, 1), sep="_")
  #outCsvPath = paste(hourlyDir, outFileName, sep = "\\")
  
  # we are reading the first line from the file in order to set some variables:
  firstRow = dat[1,]
  dt1 = as.POSIXct(dat$time[1],format="%Y-%m-%d %H:%M:%S",tz=Sys.timezone(),origin="1970-01-01 00:00.00 UTC")

  curHour = format(dt1, "%H")
  curDay = format(dt1, "%d")
  
  curPartition = dat$partition[1]
  
  print(curPartition)
  
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
        outputDataFrame <- rbind(outputDataFrame, data.frame("time" = newDt, "value" = 0, "partition" = curPartition))
      } else {
        val = as.numeric(format((as.numeric(sum) / as.numeric(counter)), digits=1, nsmall=1))

        outputDataFrame <- rbind(outputDataFrame, data.frame("time" = newDt, "value" = val, "partition" = curPartition))
      }
      
      curPartition = dat$partition[i]
      sum = as.numeric(dat$value[i])
      sum = ifelse(!is.na(sum), sum, prevVal)
      counter = 1

    }
    tmp = as.numeric(dat$value[i])
    if (!is.na(tmp)) {  # prevVal will store the last value that could have been converted to float, nothing else we can do here
      prevVal = tmp
    }
  }
  
  result = data.frame(outputDataFrame, stringsAsFactors = FALSE) 
  #write.csv(result, file = outCsvPath, row.names=FALSE, quote = FALSE)
  
  return(result)
}

# This procedure takes a list of spark data frames, combines them into one (as separate partitions) and feeds 
# them to the spark cluster for processing. Afterwards, the result is split and stored into CSV files
executeAndWrite <- function(station, measurement, dat, destDir) {
  if (!is.element(measurement, tableNames(station))) {
    hourlyDir = paste(destDir, "hourlyR", sep = "\\")
    createdDir = ifelse(!dir.exists(hourlyDir), dir.create(hourlyDir), FALSE)
  
    
    
    partitionNo = length(dat)
    inputDat = list()
    
    for (i in 1:partitionNo){
      #print(typeof(dat[i]))
      tmpDat = read.csv(file=dat[[i]], header = FALSE)
      colnames(tmpDat) = c("time", "timeDiscarded", "value")
      dt1 = as.POSIXct(tmpDat$time[1],format="%Y-%m-%d %H:%M:%S",tz=Sys.timezone(),origin="1970-01-01 00:00.00 UTC")
      
      year = format(dt1, "%Y")
      month = format(dt1, "%m")
      
      partitionCol = matrix(i-1, nrow = length(tmpDat$time), ncol = 1)
      
      singlePartitionDat = data.frame(tmpDat, partitionCol, stringsAsFactors = FALSE)
      
      colnames(singlePartitionDat) = c("time", "timeDiscarded", "value", "partition")
      
      inputDat = rbind(inputDat, singlePartitionDat)
    }
    
    #createDataFrame(testDatMerged) %>% createOrReplaceTempView("temperature")
    #sql("CREATE TABLE measurements3 USING csv PARTITIONED BY(partition) AS SELECT * FROM temperature")
    
    df <- createDataFrame(inputDat) # represent as Spark data frame
    #repartitionedDf <- repartition(df, col = df$partition)
    
    schema <- structType(structField("time", "timestamp"), structField("value", "double"), structField("partition", "double"))
    
    #processedDf <- dapply(repartitionedDf, function(key, x) { data.frame(produceHourlyResults(x), stringsAsFactors = FALSE) }, schema)
      
    processedDf <- gapply(df, "partition", function(key, x) { data.frame(produceHourlyResults(x), stringsAsFactors = FALSE) }, schema)
    
    processedDf %>% createOrReplaceTempView("data_temp")
    
    sql(paste("DROP TABLE IF EXISTS", measurement))
    sql(paste("CREATE TABLE", measurement ,"USING csv PARTITIONED BY(partition) AS SELECT * FROM data_temp"))
    
    #outFileName = paste(partitionNo,".csv",sep = "")
    #outCsvPath = paste(hourlyDir, outFileName, sep = "\\")
    
    #print(outCsvPath)  
    
    #write.df(drop(processedDf, processedDf$partition), outCsvPath, source = "csv", mode = "overwrite")  
      
    return(processedDf)
  } else {
    return(NULL)
  }
}

initSparkSession <- function(driverMemory) {
  Sys.setenv(SPARK_HOME="C:\\spark-2.1.1-bin-hadoop2.7")
  .libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
  library("SparkR", lib.loc="C:\\spark-2.1.1-bin-hadoop2.7\\R\\lib") # The use of \\ is for windows environment
  library(SparkR)
  
  #sparkR.session.stop()
  
  sc <- sparkR.session(master="local[*]", 
                       enableHiveSupport = TRUE,  
                       sparkConfig = list(spark.sql.shuffle.partitions="4",
                                          spark.sql.warehouse.dir = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\Data\\s000\\humi\\hourlyR"))
  
  
  # just a bunch of settings for the spark & hive cluster
  library(magrittr)
  #sql("CREATE DATABASE IF NOT EXISTS METEO")
  #sql("USE METEO")
  
  return(sc)
}

start.time <- Sys.time()

numOfCores = 4 # This is environment dependant, should probably be stored somewhere (a prop file?)
sc <-initSparkSession("4g")

options(stringsAsFactors = FALSE) #this is a global setting for this shit

dataDir = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\Data"
stations = list.files(dataDir)
for(station in stations){
  
  sql(paste("CREATE DATABASE IF NOT EXISTS",station))
  sql(paste("USE", station))
  
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
        numOfCores = length(months)
        if (len < numOfCores) {
          dfList[[len + 1]] <- filePath
        } else {
          # This means we have reached the parallel capacity and can now proceed with our batch
          print(paste("Processing batch:", station, measurement, "of size" ,length(dfList)))
          dupa2 <- executeAndWrite(station, measurement, dfList, subSubDir)
          
          end.time <- Sys.time()
          time.taken <- end.time - start.time
          print(time.taken)
          
          #stop("!!!")
          
          dfList <- list(filePath)
        }
      } 
    }
    
    if (length(dfList) != 0) {# We have to send the last batch to spark (in case there is still something not processed)
      print(paste("Processing remaining batch:", station, measurement, "of size" ,length(dfList)))
      executeAndWrite(station, measurement, dfList, subSubDir)
      
      end.time <- Sys.time()
      time.taken <- end.time - start.time
      print(time.taken)
      
      #stop("***")
    }
    
  }
}




# procFile = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\Data\\s018\\winds\\s018_winds_2017-07-01_2017-07-31.csv"
# procFile2 = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\Data\\s018\\winds\\s018_winds_2017-06-01_2017-06-30.csv"
# procFile3 = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\Data\\s018\\winds\\s018_winds_2017-05-01_2017-05-31.csv"
# 
# 
# dat = read.csv(file=procFile, header = FALSE)
# dat2 = read.csv(file=procFile2, header = FALSE)
# dat3 = read.csv(file=procFile3, header = FALSE)
# 
# #dat = read.df(procFile, "csv", header = "false", inferSchema = "true", na.strings = "NA")
# colnames(dat) = c("time", "timeDiscarded", "value") 
# colnames(dat2) = c("time", "timeDiscarded", "value")
# colnames(dat3) = c("time", "timeDiscarded", "value")
# 
# randomVector0 = matrix(0, nrow = length(dat$time), ncol = 1)
# randomVector1 = matrix(1, nrow = length(dat2$time), ncol = 1)
# randomVector2 = matrix(2, nrow = length(dat3$time), ncol = 1)
# 
# 
# testDat = data.frame(dat, randomVector0)
# testDat2 = data.frame(dat2, randomVector1)
# testDat3 = data.frame(dat3, randomVector2)
# 
# 
# #testDat = data.frame(dat, randomVector)
# colnames(testDat) = c("time", "timeDiscarded", "value", "partition")
# colnames(testDat2) = c("time", "timeDiscarded", "value", "partition")
# colnames(testDat3) = c("time", "timeDiscarded", "value", "partition")
# 
# testDatMerged = rbind(testDat, testDat2, testDat3)
# 
# df <- as.DataFrame(testDatMerged)
# 
# #head(select(dat, dat$time, spark_partition_id()))
# 
# #head(summarize(groupBy(df, spark_partition_id()), count = n(spark_partition_id())))
# 
# newDf <- repartition(df, 3L, col = df$partition)
# 
# #head(summarize(groupBy(newDf, spark_partition_id()), count = n(spark_partition_id())))
# 
# #head(filter(newDf, spark_partition_id() == 0))
# 
# #schema <- structType(structField("time", "double"), structField("value", "double"))
# 
# ldf3 <- dapplyCollect(newDf, function(x) { x <- produceHourlyResults(x, procFile, subSubDir) })
# #head(ldf3, 3)
# 
# 
# end.time <- Sys.time()
# time.taken <- end.time - start.time
# time.taken
# 
# 
# #result <- produceHourlyResults(filePath, subSubDir)