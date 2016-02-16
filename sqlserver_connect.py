#!/usr/bin/python
#Author: Tapas Sharma
#Simple script to test connection to a SQL Server
#TODO: Add the field to take dbname as a parameter
import pyodbc
import socket
import sys
from datetime import datetime

#creates a connection string that we can use to connect to the db server
#using FreeTDS and UnixODBC, setting TDS_VERSION to 7.2 since we do not support 7.1
def get_connection_string(server_ip, server_port, username, password):
    return "DRIVER={FreeTDS};SERVER=%s;PORT=%s;UID=%s;PWD=%s;TDS_VERSION=7.2;" \
               % (server_ip, str(server_port),username, password)

#returns a connection object to use to fire queries
def get_connection(server_ip, port, username, password, max_retry=3):
    conn_str = get_connection_string(server_ip, port, username, password)
    retry = 0
    conn = None
    while retry < max_retry:
        try:
            print "LOG: Checking with socket to see if the socket is open and reachable"
            #done to not stick in the tcp timeout of 15 mins which is a limitation in pyodbc
            #check with socket to connect with mssql server
            test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            test_socket.settimeout(1)
            test_socket.connect((server_ip, port))
            print "INFO: Socket connection sucessful..."
        except socket.error:
            # get the error number and error string from sys.exc_info()
            errno, errstr = sys.exc_info()[:2]
            if errno == socket.timeout:
                print "ERROR: Timeout has occured...", errstr
            return conn
        except Exception, ex:
            print ex
            return conn
        finally:
            if test_socket:
                test_socket.close()
        print "LOG: Checking with pyodbc"
        try:
            conn = pyodbc.connect(conn_str, timeout=5)
            break
        except Exception, ex:
            retry = retry + 1
            print "ERROR: Was Not Able To Connect  ", ex
    print "INFO: Connected to Server", server_ip, "Successfully"
    #Set the default query timeout on the connection
    if conn:
        conn.timeout = 5
    return conn

#open a cursor and then execute the supplied query
def execute_query(conn, query):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        return results
    except Exception, ex:
        return ex

def main():
    if len(sys.argv) <> 6:
        print "Error(%d): Please use the script as follows" % len(sys.argv)
        print "Usage: python sqlserver_connect.py DB_IP DB_PORT Username \'Password\' \"QUERY\""
        return
    ip = sys.argv[1]
    port = int(sys.argv[2])
    username = sys.argv[3]
    password = sys.argv[4]
    query = sys.argv[5]
    conn = get_connection(ip, port, username, password)
    start_time = datetime.now()
    print "Results:\n", execute_query(conn, query)
    end_time = datetime.now()
    print "Started at: ", start_time
    print "Finished at: ", end_time
    conn.close()

if __name__ == '__main__':
    main()
