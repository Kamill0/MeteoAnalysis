import os
import sys
import datetime

import ibm_db
from ibm_db import connect
from ibm_db import active


import logging

LOG_FILENAME = "dashDB_Logs.log"
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,format='%(asctime)s, %(levelname)s, %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

class DashDB:
    def __init__(self):
        self.connection = None
        self.userid = "dash5772"
        self.password = "ac6488d7b729"
        self.hostname = "dashdb-entry-yp-lon02-01.services.eu-gb.bluemix.net"
        self.portnumber = "50000"

    def connectionInit(self):
        self.databaseConnectionInfo = {"Database name": "BLUDB", "User ID": self.userid, "Password": self.password,
                                       "Host name": self.hostname, "Port number": self.portnumber}
        self.DatabaseSchema = 'DASH' + self.userid[4:]

        dbtry = 0
        while (dbtry <3):
            try:
                if 'VCAP_SERVICES' in os.environ:
                    hasVcap = True
                    import json
                    vcap_services = json.loads(os.environ['VCAP_SERVICES'])
                    if 'dashDB' in vcap_services:
                        hasdashDB = True
                        service = vcap_services['dashDB'][0]
                        credentials = service["credentials"]
                        url = 'DATABASE=%s;uid=%s;pwd=%s;hostname=%s;port=%s;' % ( credentials["db"],credentials["username"],credentials["password"],credentials["host"],credentials["port"])
                    else:
                        hasdashDB = False
                else:
                    hasVcap = False
                    self.url = 'DATABASE=%s;uid=%s;pwd=%s;hostname=%s;port=%s;' % (
                    self.databaseConnectionInfo["Database name"], self.databaseConnectionInfo["User ID"],
                    self.databaseConnectionInfo["Password"], self.databaseConnectionInfo["Host name"],
                    self.databaseConnectionInfo["Port number"])
                print(self.url)
                self.connection = ibm_db.pconnect(self.url, '', '')
                if (active(self.connection)):
                    print(self.connection, "SUCCESSFULLY CONNECTED")
                    return self.connection
            except Exception as e:
                print("Error")
                logging.error("Exception occured %s" % (ibm_db.conn_errormsg()))
                logging.error("Exception occured %s" % e)
                dbtry += 1

                # Function to close the dashdb connection
    def dbclose(self):

        try:
            retrn = ibm_db.close(self.connection)
            return retrn
        except Exception as dbcloseerror:
            logging.error("dbclose Exception %s" % (dbcloseerror))
            return False

    # Function to check whether the connection is alive or not
    def connectioncheck_handler(self):
        try:
            logging.info("connection is" + str(active(self.connection)))
            dbretry = 0
            if (active(self.connection) == False):
                while (dbretry < 3):
                    self.connection = ibm_db.connect(self.url, '', '')
                    if active(self.connection) == True:
                        dbretry = 3
                    else:
                        if dbretry == 2:
                            raise Exception("db retry Error")
                        else:
                            dbretry += 1

                logging.info("restarted connection is" + str(active(self.connection)))
        except Exception as e:
            logging.error("The connectioncheck_handler error is %s" % (e))

    # Function to create the Table
    def dbCreate(self, tablename, cols = {}):
        self.connectioncheck_handler()

        try:
            create_query = "CREATE TABLE " + tablename + " ( id INT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 NO MAXVALUE NO CYCLE NO CACHE ORDER),"
            for key, value in cols.iteritems():
                create_query += key + " " + value + ","

            create_query += " PRIMARY KEY(id))ORGANIZE BY ROW;"
            statement = ibm_db.exec_immediate(self.connection, create_query)
            ibm_db.free_stmt(statement)

        except Exception as e:
            logging.error("The dbCreate operation error is %s" % (e))
            return False
        except:
            logging.error("The dbCreate operation error is %s" % (ibm_db.stmt_errormsg()))
            return False
        return True

    def dbDrop(self, tablename):
        self.connectioncheck_handler()
        try:
            create_query = "DROP TABLE " + tablename + ";"
            statement = ibm_db.exec_immediate(self.connection, create_query)
            ibm_db.free_stmt(statement)

        except Exception as e:
            logging.error("The dbDrop operation error is %s" % (e))
            return False
        except:
            logging.error("The dbDrop operation error is %s" % (ibm_db.stmt_errormsg()))
            return False
        return True


    # Function to Insert data to the created table
    def dbInsert(self, tablename, values):
        self.connectioncheck_handler()
        try:
            insert_query = "INSERT INTO " + self.DatabaseSchema + "." + tablename + " VALUES (DEFAULT"
            for value in values:
                insert_query += ",\'" + value + "\'"
            insert_query += ")"
            statement = ibm_db.exec_immediate(self.connection, insert_query)
            ibm_db.free_stmt(statement)

        except Exception as e:
            logging.error("The dbInsert operation error is %s" % (e))
            return False
        except:
            logging.error("The dbInsert operation error is %s" % (ibm_db.stmt_errormsg()))
            return False
        return True

    # Function to update the Table value
    def dbUpdate(self, tablename, columnName, updatevalue, conditionColumnName1, conditionColumnValue1,
                 conditionColumnName2, conditionColumnValue2):
        self.connectioncheck_handler()

        try:
            update_query = "UPDATE " + tablename + " SET " + columnName + " = \'" + str(
                updatevalue) + "\'  WHERE " + conditionColumnName1 + " = \'" + str(
                conditionColumnValue1) + "\' AND " + conditionColumnName2 + " = \'" + str(
                conditionColumnValue2) + "\'"

            statement = ibm_db.exec_immediate(self.connection, update_query)
            ibm_db.free_stmt(statement)

        except Exception as e:
            logging.error("The dbUpdate operation error is %s" % (e))
            return False
        except:
            logging.error("The dbUpdate operation error is %s" % (ibm_db.stmt_errormsg()))
            return False
        return True

    # Function to Delete a row from the Table
    def dbDelete(self, tablename, conditionColumnName1, conditionColumnValue1, conditionColumnName2,
                 conditionColumnValue2):
        self.connectioncheck_handler()
        try:
            delete_query = "DELETE FROM " + self.DatabaseSchema + "." + tablename + " WHERE " + conditionColumnName1 + " = \'" + conditionColumnValue1 + "\' AND " + conditionColumnName2 + " = \'" + conditionColumnValue2 + "\' "
            statement = ibm_db.exec_immediate(self.connection, delete_query)
            ibm_db.free_stmt(statement)

        except Exception as e:
            logging.error("The dbDelete operation error is %s" % (e))
            return False
        except:
            logging.error("The dbDelete operation error is %s" % (ibm_db.stmt_errormsg()))
            return False
        return True

    # Function to Fetch the Data from the Table
    def dbFetch(self, tablename):
        self.connectioncheck_handler()
        try:
            fetch_query = "SELECT * FROM " + self.DatabaseSchema + "." + tablename + ""
            statement = ibm_db.exec_immediate(self.connection, fetch_query)
            dictionary = ibm_db.fetch_assoc(statement)
            data = []
            while (dictionary != False):
                data.append(dictionary)
                dictionary = ibm_db.fetch_assoc(statement)
            ibm_db.free_stmt(statement)

        except Exception as e:
            logging.error("The dbFetch operation error is %s" % (e))
            return False
        except:
            logging.error("The dbFetch operation error is %s" % (ibm_db.stmt_errormsg()))
            return False
        return data


if __name__ == '__main__':
    # how to use the api:
    db = DashDB()
    db.connectionInit()

    #create_retrn = db.dbCreate("USERTABLE", {"name": "VARCHAR(20) NOT NULL", "age": "VARCHAR(20)"})
    #insert_val = db.dbInsert("USERTABLE", ["Kamil", "22"])

    print(db.dbFetch("USERTABLE"))

