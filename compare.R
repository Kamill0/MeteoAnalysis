library(plotrix)

#wfiisPath = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\Data\\s000\\temp\\hourly\\h_s000_temp_2017-07-01_2017-07-31.csv"
#noaaPath = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\DataNOAA\\processed\\temp\\temp_2017-7.csv"

compareWeatherModels <- function(noaaPath, wfiisAnomsPath, wfiisNoAnomsPath) {
  
  wfiisDF = read.csv(file=wfiisNoAnomsPath, header = FALSE)
  colnames(wfiisDF) <- c('time','value')
  
  noaaDF = read.csv(file=noaaPath, header = FALSE)
  colnames(noaaDF) <- c('time','value')
  
  wfiisDF$time <- as.character(wfiisDF$time)
  noaaDF$time <- as.character(noaaDF$time)
  
  
  
  merged = merge(wfiisDF, noaaDF, by = "time")
  
  ref = as.numeric(as.character(unlist(merged[3])))
  
  model = as.numeric(as.character(unlist(merged[2])))
  
  
  taylor.diagram(ref,
                 model,
                 add=FALSE,
                 col="red",
                 pch=4,
                 pos.cor=TRUE,
                 xlab="MERRA SD (Normalised)",
                 ylab="RCA4 runs SD (normalised)",
                 main="Taylor Diagram2",
                 show.gamma=TRUE,
                 ngamma=3,
                 sd.arcs=1,
                 ref.sd=TRUE,
                 grad.corr.lines=c(0.2,0.4,0.6,0.8,0.9),
                 pcex=1,cex.axis=1,
                 normalize=TRUE,
                 mar=c(5,4,6,6),
                 lwd=10,
                 font=5,
                 lty=3)  
}

# this function will look for the wfiis data from the peroid that matches the noaa observations
lookForWfiisData <- function(noaaFileName, targetDir, measurement) {
  wfiisFileToLookFor = gsub("sNOAA", "s000", noaaFileName)
  targetDir = paste(targetDir, measurement, "hourly", sep = "\\")
  availableMonths = list.files(targetDir, include.dirs = FALSE)
  return(ifelse((wfiisFileToLookFor %in% availableMonths), paste(targetDir, wfiisFileToLookFor, sep = "\\"), NA))
}


rootDir = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis"
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
     noaaPath = paste(subDir, month, sep = "\\")
     
     
     compareWeatherModels(noaaPath, matchingResultAnoms, matchingResultNoAnoms)
     stop("!!!")
   } else {
     print(paste("There was no match in WFiiS data records for Noaa file:", month, "- skipping"))
   }
    
  }
}
