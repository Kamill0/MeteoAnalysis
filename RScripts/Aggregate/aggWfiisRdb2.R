library(ibmdbR)

produceHourlyResults <- function(dat) {
  
  outputDataFrame = setNames(data.frame(matrix(ncol = 2, nrow = 0)), c("time", "value"))
  
  
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
  return(result)
}

driver.name <- "IBMDBDRIVER"
db.name <- "BLUDB"


host.name <- "dashdb-entry-yp-lon02-01.services.eu-gb.bluemix.net"
port <- 50000
user.name <- "dash9493"
pwd <- "lJnTTc@P$07z"

con.text <- paste("IBMDBDRIVER;DRIVER=",driver.name,
                  ";Database=",db.name,
                  ";Hostname=",host.name,
                  ";Port=",port,
                  ";PROTOCOL=TCPIP",
                  ";UID=", user.name,
                  ";PWD=",pwd,sep="")
# Connect to using a odbc Driver Connection string to a remote database
con <- idaConnect(con.text)
idaInit(con)
tables <- idaShowTables()

dataDir = "C:\\Users\\kamil_000\\PycharmProjects\\MeteoAnalysis\\DataFromDb2"

start.time <- Sys.time()

for (name in tables$Name){
  df <- idaQuery(paste('SELECT * FROM DASH9493."',name,'" ORDER BY TIME', sep=""))
  colnames(df) = c("time", "timeDiscarded", "value")
  result <- produceHourlyResults(df)
  outCsvPath <- paste(dataDir, name, sep="\\")
  write.csv(result, file = outCsvPath, row.names=FALSE, quote = FALSE)
  
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  print(time.taken)
}



