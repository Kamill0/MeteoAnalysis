library(AnomalyDetection)

#data(raw_data)
#res = AnomalyDetectionTs(raw_data, max_anoms=0.02, direction='both', plot=TRUE)
#res$plot

dat = read.csv(file="C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\Data\\2017-07-31.csv", header = FALSE)
colnames(dat) <- c('time','time_meh','temp')
dat <- dat[ -c(2)]
dat$time <- as.POSIXct(paste(dat$time), format="%Y-%m-%d %H:%M:%S")
dat <- dat[!duplicated(dat),]
dat$temp[1] = dat$temp[1]*2  #fake anomaly creation <- it works, yay 
res = AnomalyDetectionTs(dat, max_anoms=0.02, direction='both', plot=TRUE)
if (is.null(res$plot)){
  print("No anomalies detected, nothing to plot here")
} else {
  res$plot
}




