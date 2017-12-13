
require(scales)
# Will look for anomalies in the collected measurements using Twitter's Anomaly Detection library

produceHourlyResults <- function(procFile, destDir) {
  dat = read.csv(file=procFile, header = FALSE)
  colnames(dat) = c("time", "timeDiscarded", "value")  
  
  createdDir = ifelse(!dir.exists(test1), dir.create(test1), FALSE)
  
  # we are reading the first line from the file in order to set some variables:
  firstRow = dat[1,]
  dt1 = as.POSIXct(dat$time[1],format="%Y-%m-%d %H:%M:%S",tz=Sys.timezone())
  curHour = format(dt1, "%H")
  curDat = forma(dt1, "%d")
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
      
    }
    #dosomething(df[i,])
  }
  
  
  return(result)
}

dataDir = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\Data"
#anomalyDir = paste(dataDir, "DetectedAnomalies", sep = "\\")
stations = list.files(dataDir)
for(station in stations){
  subDir = paste(dataDir,station,sep = "\\")
  typeOfMeasurment = list.files(subDir)
  for(measurement in typeOfMeasurment){
    subSubDir = paste(subDir,measurement,sep = "\\")
    months = list.files(subSubDir)
    for(month in months){
      filePath = paste(subSubDir,month,sep="\\")
      
      result <- produceHourlyResults(filePath, subSubDir)
      
      print(filePath)
      
      
      colnames(dat) <- c('time','timeDiscarded','value')
      dat <- dat[ -c(2)]
      dat$time <- as.POSIXct(paste(dat$time), format="%Y-%m-%d %H:%M:%S")
      dat <- dat[!duplicated(dat),]
      #dat$value[1] = dat$value[1]*2  #fake anomaly creation <- it works, yay
      dat <- na.omit(dat)
      bol = is.na(dat)
      tryCatch(
        {
          res = AnomalyDetectionTs(dat, max_anoms=0.02, direction='both', plot=TRUE, na.rm = TRUE)
          if (is.null(res$plot)){
            # print("No anomalies detected, nothing to plot here")
            # do something here, print is useless
          } else {
            fileName = paste(sapply(strsplit(filePath,split='\\',fixed=TRUE), tail, 1),".png", sep="")
            path = paste(anomalyDir,fileName,sep="\\")
            png(filename=path)
            plot(res$plot + ylab(measurement) + scale_x_datetime(breaks = date_breaks("10 days"), labels=date_format("%Y-%m-%d"), limits=xlim))
            dev.off()
          }
        }, error = function(e) {
          message("Here's the original error message: ")
          message(e)
          message(paste("\nfor:", fileName)) 
        }
      )
    }
  }
}







