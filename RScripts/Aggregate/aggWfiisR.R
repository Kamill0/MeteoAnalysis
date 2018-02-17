
require(scales)
# This script takes WFiIS data and produces a file with hourly results 

produceHourlyResults <- function(procFile, destDir) {
  dat = read.csv(file=procFile, header = FALSE)
  if (length(dat) == 3) {
    colnames(dat) = c("time", "timeDiscarded", "value")
  } else if (length(dat) == 2) {
    colnames(dat) = c("time", "value")
  } else {
    print(paste("This is not a valid data file: ", procFile))
  }
  
  outputDataFrame = setNames(data.frame(matrix(ncol = 2, nrow = 0)), c("time", "value"))
  
  
  hourlyDir = paste(destDir, "hourly", sep = "\\")
  createdDir = ifelse(!dir.exists(hourlyDir), dir.create(hourlyDir), FALSE)
  outFileName = paste("h", sapply(strsplit(procFile,split='\\',fixed=TRUE), tail, 1), sep="_")
  outCsvPath = paste(hourlyDir, outFileName, sep = "\\")
  
  # we are reading the first line from the file in order to set some variables:
  firstRow = dat[1,]
  dt1 = as.POSIXct(dat$time[1],format="%Y-%m-%d %H:%M:%S",tz=Sys.timezone())
  curHour = format(dt1, "%H")
  curDay = format(dt1, "%d")
  sum = 0
  counter = 0
  prevVal = 0

  
  for (i in 2:nrow(dat)){
    dt = as.POSIXct(dat$time[i],format="%Y-%m-%d %H:%M:%S",tz=Sys.timezone())
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
    if (!is.na(tmp)) {  # prevVal will store the last value that could have been converted to float,
      prevVal = tmp
    }
  }
  
  result = outputDataFrame
  write.csv(result, file = outCsvPath, row.names=FALSE, quote = FALSE)
  return(result)
}


dataDir = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\Data"
stations = list.files(dataDir)
for(station in stations){
  subDir = paste(dataDir,station,sep = "\\")
  typeOfMeasurment = list.files(subDir)
  for(measurement in typeOfMeasurment){
    subSubDir = paste(subDir,measurement,sep = "\\")
    months = list.files(subSubDir, include.dirs = FALSE)
    for(month in months){
      filePath = paste(subSubDir,month,sep="\\")
      if (file_test("-f", filePath)) {
        print(paste("Processing file:", month))
        result <- produceHourlyResults(filePath, subSubDir)

        outputDataFrame <- rbind(outputDataFrame, data.frame("sql" = a), data.frame("sql" = b))
  
      } else {
        print("Not a regular file, skipping")
      }
      
    }
  }
}







