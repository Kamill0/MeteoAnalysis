library(plotrix)


#wfiisPath = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\Data\\s000\\temp\\hourly\\h_s000_temp_2017-07-01_2017-07-31.csv"
#noaaPath = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\DataNOAA\\processed\\temp\\temp_2017-7.csv"

compareWeatherModels <- function(fileName, noaaPath, wfiisAnomsPath, wfiisNoAnomsPath) {
  
  wfiisNoAnomsDF = read.csv(file=wfiisNoAnomsPath, header = FALSE)
  colnames(wfiisNoAnomsDF) <- c('time','value')
  
  wfiisAnomsDF = read.csv(file=wfiisAnomsPath, header = FALSE)
  colnames(wfiisAnomsDF) <- c('time','value')
  
  noaaDF = read.csv(file=noaaPath, header = FALSE)
  colnames(noaaDF) <- c('time','value')
  
  print(noaaDF$value)
  
  wfiisNoAnomsDF$time <- as.character(wfiisNoAnomsDF$time)
  wfiisAnomsDF$time <- as.character(wfiisAnomsDF$time)
  noaaDF$time <- as.character(noaaDF$time)
  
  
  mergedAnoms = merge(wfiisAnomsDF, noaaDF, by = "time")
  mergedNoAnoms = merge(wfiisNoAnomsDF, noaaDF, by = "time")
  
  refNoAnoms = as.numeric(as.character(unlist(mergedNoAnoms[3])))
  refAnoms = as.numeric(as.character(unlist(mergedAnoms[3])))
  
  print(refNoAnoms)
  
  modelNoAnoms = as.numeric(as.character(unlist(mergedNoAnoms[2])))
  modelAnoms = as.numeric(as.character(unlist(mergedAnoms[2])))
  
  
  pngFileName = paste(tools::file_path_sans_ext(fileName),".png", sep="")
  pathPng = paste(pathTaylorPng,pngFileName,sep="\\")
  
  png(filename=pathPng)
  
  taylor.diagram(refAnoms,
                 modelAnoms,
                 add=FALSE,
                 col="red",
                 pos.cor=F,
                 xlab="Standard deviation",
                 ylab="Standard deviation",
                 main="Taylor Diagram",
                 show.gamma=TRUE,
                 ngamma=3,
                 sd.arcs=1,
                 ref.sd=TRUE,
                 grad.corr.lines=c(0.2,0.4,0.6,0.8,0.9),
                 pcex=1.3,cex.axis=1,
                 normalize=TRUE,
                 mar=c(5,4,6,6),
                 lwd=10,
                 font=5,
                 lty=3)  
  
  taylor.diagram(refNoAnoms,
                 modelNoAnoms,
                 add=TRUE,
                 col="blue",
                 pos.cor=TRUE,
                 show.gamma=TRUE,
                 ngamma=3,
                 sd.arcs=1,
                 ref.sd=TRUE,
                 grad.corr.lines=c(0.2,0.4,0.6,0.8,0.9),
                 pcex=1.3,cex.axis=1,
                 normalize=TRUE,
                 mar=c(5,4,6,6),
                 lwd=10,
                 font=5,
                 lty=3)  
  
  model.names <- c("Data with anomalies","Data without anomalies","NOAA model")
  model.colors <- c("red", "blue", "darkgreen")
  model.pch <- c(19,19,15)
  legend("topright", model.names, pch=model.pch, col=model.colors , cex=1.0, bty="n", ncol=1, xpd = TRUE)
  
  dev.off()
}

# this function will look for the wfiis data from the peroid that matches the noaa observations
lookForWfiisData <- function(noaaFileName, targetDir, measurement) {
  wfiisFileToLookFor = gsub("sNOAA", "s000", noaaFileName)
  targetDir = paste(targetDir, measurement, "hourly", sep = "\\")
  availableMonths = list.files(targetDir, include.dirs = FALSE)
  return(ifelse((wfiisFileToLookFor %in% availableMonths), paste(targetDir, wfiisFileToLookFor, sep = "\\"), NA))
}


rootDir = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis"
pathTaylorPng = paste(rootDir, "TaylorComparison", sep = "\\")
noaaDataRootDir = paste(rootDir, "DataNOAA\\processed", sep = "\\")
wfiisAnomsDataRootDir = paste(rootDir, "Data\\s000", sep = "\\")
wfiisNoAnomsDataRootDir = paste(rootDir, "DataWithoutAnoms\\s000", sep = "\\")

measurements = list.files(noaaDataRootDir)
for(measurement in measurements){
  subDir = paste(noaaDataRootDir,measurement,sep = "\\")
  months = list.files(subDir, include.dirs = FALSE)
  for(month in months){
   matchingResultAnoms = lookForWfiisData(month, wfiisAnomsDataRootDir, measurement)
   matchingResultNoAnoms = lookForWfiisData(month, wfiisNoAnomsDataRootDir, measurement) #FIX folder names for that
   
   if(!is.na(matchingResultAnoms) && !is.na(matchingResultNoAnoms) ) {
     print(paste("Processing:", month))
     noaaPath = paste(subDir, month, sep = "\\")
     
     
     compareWeatherModels(month, noaaPath, matchingResultAnoms, matchingResultNoAnoms)
     stop("!!!")
   } else {
     print(paste("There was no match in WFiiS data records for Noaa file:", month, "- skipping"))
   }
    
  }
}
