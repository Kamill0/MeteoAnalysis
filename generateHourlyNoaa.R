type_of_measurement <- c(23,21,22,4)
names(type_of_measurement) <- c("pres0", "temp", "temp0", "winds")

getFileName <- function(measurement, date) {
  year = format(date, "%Y")
  month = format(date, "%m")
  
  firstDay = as.POSIXct(paste(year, month, "01"),format="%Y %m %d",tz=Sys.timezone())
  nextMon = seq(firstDay, length=2, by='1 month')[2]
  
  lastDay <- seq(nextMon, length=2, by='-1 day')[2]
  
  return(paste("h", "sNOAA", measurement, as.character(firstDay), as.character(lastDay), sep = "_"))
}

rootDir = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis"

#a <- type_of_measurement["temp"]

#parseFormattedFile <- function(measurement) {
  
  
#}

calculateValue <- function(measurement, line) {
  if (startsWith(measurement, "temp0")) {
    value = as.numeric((line$DEWP-32) * 5/9 )
    #value = ifelse(!is.na(sum), sum, prevVal)
  } else if (startsWith(measurement, "temp")) {
    value = as.numeric((line$TEMP-32) * 5/9 )
  } else if (startsWith(measurement, "winds")) {
    value = as.numeric(line$SPD * 0.44704)
  } else {
    value = as.numeric(line$SLP)
  }
 return(value) 
}

measurement = "temp" # TODO REMOVE THIS

outputPath <- paste(rootDir, "DataNOAA\\processed", sep = "\\")
inputPath <- paste(rootDir, "DataNOAA\\source", sep = "\\")

procFile <- "2017_balice.txt" # TODO for removal

dat = read.table(file=paste(inputPath, procFile, sep="\\"), header = TRUE, colClasses = c(rep("factor",3)))

prevDt = as.POSIXct(dat[1,]$YR..MODAHRMN,format="%Y%m%d%H%M",tz=Sys.timezone(),origin="1970-01-01 00:00.00 UTC")
prevHour = format(prevDt, "%H")

sum = 0
counter = 0

outputDataFrame = setNames(data.frame(matrix(ncol = 2, nrow = 0), stringsAsFactors = FALSE), c("time", "value"))

for (i in 1:nrow(dat)){
  line = dat[i,]
  
  dt = as.POSIXct(line$YR..MODAHRMN,format="%Y%m%d%H%M",tz=Sys.timezone(),origin="1970-01-01 00:00.00 UTC")
  curHour = format(dt, "%H")
  
  if (format(dt, "%m") == format(prevDt, "%m")) {
    if(curHour == prevHour) {
      tmpVal = calculateValue(measurement, line)
      
      if (!is.na(tmpVal)) {
        sum = sum + tmpVal
        counter = counter + 1
      }
    } else {
      
      if (counter == 0) {
        outputDataFrame <- rbind(outputDataFrame, data.frame("time" = dt, "value" = 0))
      } else {
        val = as.numeric(format((as.numeric(sum) / as.numeric(counter)), digits=1, nsmall=1))
        outputDataFrame <- rbind(outputDataFrame, data.frame("time" = dt, "value" = val))
      }
      
      tmpVal = calculateValue(measurement, line)
      if (!is.na(tmpVal)) {
        sum = as.numeric(tmpVal)
        counter = 1
      } else {
        sum = 0
        counter = 0
      }
    }
  } else {
    
    if (counter == 0) {
      outputDataFrame <- rbind(outputDataFrame, data.frame("time" = dt, "value" = 0))
    } else {
      val = as.numeric(format((as.numeric(sum) / as.numeric(counter)), digits=1, nsmall=1))
      outputDataFrame <- rbind(outputDataFrame, data.frame("time" = dt, "value" = val))
    }
    
    outMeasurementPath = paste(outputPath, measurement, sep = "\\")
    ifelse(!dir.exists(outMeasurementPath), dir.create(outMeasurementPath), FALSE)
    outCsvPath = paste(outMeasurementPath, getFileName(prevDt), sep = "\\")
    
    result = outputDataFrame
    write.csv(result, file = outCsvPath, row.names=FALSE, quote = FALSE)
    
    outputDataFrame = setNames(data.frame(matrix(ncol = 2, nrow = 0), stringsAsFactors = FALSE), c("time", "value"))
    
    # TODO add current row here as first
    
    tmpVal = calculateValue(measurement, line)
    if (!is.na(tmpVal)) {
      sum = as.numeric(tmpVal)
      counter = 1
    } else {
      sum = 0
      counter = 0
    }
    
    
  }
  prevDt = dt
  prevHour = format(prevDt, "%H")
  
}



