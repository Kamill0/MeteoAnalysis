library(ibmdbR)

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
idaShowTables()

idf <- idaQuery('SELECT * FROM DASH9493."s000_temp_2017-01-01_2017-01-31.csv"')

