wfiisPath = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\Data\\s000\\temp\\hourly\\h_s000_temp_2017-07-01_2017-07-31.csv"
noaaPath = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\DataNOAA\\processed\\temp\\temp_2017-7.csv"


wfiisDF = read.csv(file=wfiisPath, header = FALSE)
colnames(wfiisDF) <- c('time','value')

noaaDF = read.csv(file=noaaPath, header = FALSE)
colnames(noaaDF) <- c('time','value')

merged = merge(wfiisDF, noaaDF, by = "time")