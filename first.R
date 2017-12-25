library(AnomalyDetection)
require(scales)
require(ggplot2)
# Will look for anomalies in the collected measurements using Twitter's Anomaly Detection library

dataDir = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\Data"
#anomalyDir = paste(dataDir, "DetectedAnomalies", sep = "\\")
anomalyDir = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\DetectedAnomalies"
#anomalyDir = paste(dataDir, "DetectedAnomalies", sep = "\\")
anomalyDir = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\DetectedAnomalies"

stations = list.files(dataDir)
for(station in stations){
  subDir = paste(dataDir,station,sep = "\\")
  typeOfMeasurment = list.files(subDir)
  for(measurement in typeOfMeasurment){
    subSubDir = paste(subDir,measurement,sep = "\\")
    months = list.files(subSubDir)
    for(month in months){
      filePath = paste(subSubDir,month,sep="\\")
      print(filePath)
      
      dat = read.csv(file=filePath, header = FALSE)
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
            plot(res$plot + scale_x_datetime(breaks = date_breaks("10 days"), labels=date_format("%Y-%m-%d")))
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







