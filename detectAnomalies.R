library(AnomalyDetection)
require(scales)
require(ggplot2)
# Will look for anomalies in the collected measurements using Twitter's Anomaly Detection library

dataDir = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\Data"
#anomalyDir = paste(dataDir, "DetectedAnomalies", sep = "\\")
anomalyPngDir = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\DetectedAnomalies"
anomalyCsvDir = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\DataWithoutAnoms"
ifelse(!dir.exists(anomalyCsvDir), dir.create(anomalyCsvDir), FALSE)





stations = list.files(dataDir)
for(station in stations){
  subDir = paste(dataDir,station,sep = "\\")
  subDirNoAnoms = paste(anomalyCsvDir,station,sep = "\\")
  ifelse(!dir.exists(subDirNoAnoms), dir.create(subDirNoAnoms), FALSE)
  
  typeOfMeasurment = list.files(subDir)
  for(measurement in typeOfMeasurment){
    subSubDir = paste(subDir,measurement,sep = "\\")
    subSubDirNoAnoms = paste(subDirNoAnoms,measurement,sep = "\\")
    ifelse(!dir.exists(subSubDirNoAnoms), dir.create(subSubDirNoAnoms), FALSE)    
    
    months = list.files(subSubDir)
    for(month in months){
      filePath = paste(subSubDir,month,sep="\\")
      print(filePath)
      if (file_test("-f", filePath)) {
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
            
            csvfileName = sapply(strsplit(filePath,split='\\',fixed=TRUE), tail, 1)
            pathCsv = paste(gsub("Data", "DataWithoutAnoms", subSubDir) ,paste("noAnoms",csvfileName,sep="_"),sep="\\")
            
            if (is.null(res$plot)){
              result <- dat
            } else {
              
              anomsDF <- res$anoms
              colnames(anomsDF) = c('time', 'value')
              anomsDF$time <- as.POSIXct(paste(anomsDF$time), tz = 'CET')
              
              pngFileName = paste(tools::file_path_sans_ext(csvfileName),".png", sep="")
              
              pathPng = paste(anomalyPngDir,pngFileName,sep="\\")
              
              png(filename=pathPng)
              plot(res$plot + scale_x_datetime(breaks = date_breaks("10 days"), labels=date_format("%Y-%m-%d")))
              dev.off()
              
              result = dat[ !(dat$time %in% anomsDF$time),]
            }
            
            write.csv(result, file = pathCsv, row.names=FALSE, quote = FALSE)
            
          }, error = function(e) {
            message("Here's the original error message: ")
            message(e)
            message(paste("\nfor:", fileName)) 
          }
        )
      }
    }
  }
}







