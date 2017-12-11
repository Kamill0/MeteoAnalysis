library(plotrix)

wfiisPath = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\Data\\s000\\temp\\hourly\\h_s000_temp_2017-07-01_2017-07-31.csv"
noaaPath = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\DataNOAA\\processed\\temp\\temp_2017-7.csv"


wfiisDF = read.csv(file=wfiisPath, header = FALSE)
colnames(wfiisDF) <- c('time','value')

noaaDF = read.csv(file=noaaPath, header = FALSE)
colnames(noaaDF) <- c('time','value')

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
               main="Taylor Diagram",
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