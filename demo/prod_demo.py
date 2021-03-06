#!/usr/bin/python
# This code will read a configuration from sqlite file
# depending on the status of the id, it will then spwan
# and waits for the join to happen
# Can be easily modified to do monitoring
#
import sys, socket, time
import log
import daemon
import os
import getopt
import ConfigParser
import multiprocessing
import sqlite3
import traceback
import signal

_debug = False
CWD = os.getcwd()
SCRIPT_VERSION = 1.0
GLOBAL_LB_SQLITE_FILE = CWD+'/'+'lb.sqlite'
LB_SQLITE_FILE = CWD+'/'+'lb_%s.sqlite'
SLEEP_INTERVAL = 5
MAX_RETRY = 10
gMonitoredClusters = {}

TIME_TO_WAIT_FOR_CHILD_JOIN = 60 # in seconds

STATUS_UP = 1
STATUS_DOWN = 0

# Initialize logging
log.set_logging_prefix("monitor")
_logger = log.get_logger("monitor")

# The configuration file for service
MONITOR_CONF = 'monitor.conf'

# The global variable for the configuration parser
_config = None

def _usage(msg=None):
    """Display the program's usage on stderr and exit
    """
    if msg:
        print >> sys.stderr, msg
    print >> sys.stderr, """
Usage: %s [options] [stop|restart]

Options:
    -v, --version         : Report version and exit
    -d, --debug           : Run the program in debug mode
    -h, --help            : Display help
""" % (os.path.basename(sys.argv[0]))
    sys.exit(1)

#
# Sqlite handling functions
#
def get_sqlite_handle(db_name, timeout=30):
    '''
    Returns a sqlite handle to the recieved db_name
    '''
    try:
        if timeout <= 0:
            timeout = 30
        #if timeout:
        #    conn = sqlite3.connect(db_name, timeout=timeout)
        #else:
        conn = sqlite3.connect(db_name, timeout=timeout)
        # obtain all results as python dictionaries
        conn.row_factory = sqlite3.Row
        return conn
    except:
        return None

def close_sqlite_resources(sqlite_handle, cursor):
    '''
    Close the sqlite file and release the handle
    '''
    try:
        if cursor:
            cursor.close()
        if sqlite_handle:
            sqlite_handle.close()
    except:
        pass
    
def get_config_parser(config_file, options={}):
    """Get a config parser for the given configuration file
    """
    if not os.path.isabs(config_file):
        config_file = CWD + '/' + config_file

    if not os.path.exists(config_file):
        raise Exception('Monitor: File not found: %s' % config_file)
    config = ConfigParser.SafeConfigParser(options)
    config.read(config_file)
    return config

def read_pid(pidfile):
    '''
    Check if the pid file is existing and read the pid
    '''
    if pidfile:
        if not os.path.exists(pidfile):
            return False
        try:
            pf = file(pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
            return pid
        except Exception, ex:
           _logger.error("Failed to read pid from %s"\
                                       % (pidfile,))
    return None


#
# The class that will actually maonitor the changes
# This is where we will implement the monitoring logic
#
def MonitorUtils(object):
    def __init__(self, clusterid, parent_pid):
        self._cluster_id = clusterid
        self._parent_pid = parent_pid
        _logger.info("Monitor Utlity initialized")

    def _is_parent_alive(self):
        '''
        Returns True/false
        '''
        if not os.path.exists("/var/run/monitor.pid"):
            return False

        pid_file = "/proc/" + str(self._parent_pid)
        if os.path.exists(pid_file):
            return True
        return False

    def startMonitor(self):
        while True:
            if gSignalChildToQuit:
                sys.exit()
            if not self._is_parent_alive():
                _logger.info("Monitor(%d): Parent is gone away.. " \
                             "Exiting now" % self._cluster_id)
                return

def cleanup_monitor_process(signum, frame):
    '''
    Cleanup marker file if it was found.
    '''
    global gSignalChildToQuit
    gSignalChildToQuit = True
    pid = os.getpid()
    try:
        if read_pid(gMonitorProcessMarkerFile) == pid:
            os.remove(gMonitorProcessMarkerFile)
        else:
            _logger.debug("Markerfile pid not match with the current process pid")
    except Exception, ex:
        _logger.error('Failed to remove marker file: %s' % gMonitorProcessMarkerFile)

        
def monitor_routine(cluster_id, parent_pid):
    '''
    The Main Function which does the monitoring of the server role change over
    the VNN
    '''
    # set the marker file
    global gMonitorProcessMarkerFile
    gMonitorProcessMarkerFile = "/var/run/monitor_%d.file" % cluster_id

    #  try to create marker file
    if os.path.exists(gMonitorProcessMarkerFile) == False:
        try:
            fp = open(gMonitorProcessMarkerFile, "w")
            fp.write(str(os.getpid()))
            fp.close()
        except:
            _logger.error("Monitor(%d): Failed to create marker file %s" \
                          % (cluster_id, gMonitorProcessMarkerFile))
            sys.exit(1)
    else:
        _logger.warn("Monitor(%d): Marker file already in use. " \
                     "Exiting now" % cluster_id)
        sys.exit(0)

    # resgister to handle SIGTERM & SIGHUP
    signals = [signal.SIGTERM, signal.SIGHUP]
    for s in signals:
        signal.signal(s, cleanup_monitor_process)

    mon_object = MonitorUtils(cluster_id, parent_pid)
    try:
        mon_object.startMonitor()
    except Exception, ex:
        _logger.error("Monitor(%d): Instance failed: %s" \
                      % (cluster_id, ex))
        _logger.error("%s" % (traceback.format_exc(),))

    # if we are here then we need to exit, probably because parent is gone away.
    try:
        os.remove(gMonitorProcessMarkerFile)
    except Exception, ex:
        _logger.error("Monitor(%d): Failed to remove marker " \
                        "file." % cluster_id)
    sys.exit(0)


#
# This is our monitoring class
# has dependency on the Daemon implementation
#
class MonitorDaemon(daemon.Daemon):
    """This class runs Monitor as a daemon
    """
    def get_list_of_cluster_ids(self):
        '''
        Return list of active as well as stopped cluster ids.
        '''
        running_cluster_ids = []
        stopped_cluster_ids = []
        sqlite_handle = get_sqlite_handle(GLOBAL_LB_SQLITE_FILE)
        if sqlite_handle:
            db_cursor = sqlite_handle.cursor()
            query = "select status,id from lb_summary where status<>9;"
            retry = 0
            while retry < MAX_RETRY:
                try:
                    db_cursor.execute(query)
                    for row in db_cursor.fetchall():
                        if int(row['status']) == STATUS_UP:
                            running_cluster_ids.append(int(row['id']))
                        else:
                            stopped_cluster_ids.append(int(row['id']))
                    break
                except Exception, ex:
                    retry = retry + 1
                    if retry >= MAX_RETRY:
                        _logger.error("Failed to find list of all clusters: %s" % ex)
                    else:
                        time.sleep(0.1)

            close_sqlite_resources(sqlite_handle, db_cursor)
        return running_cluster_ids, stopped_cluster_ids

    def _read_status(self, clusterid):
        '''
        Check whether cluster is our type or not from sqlite of each cluster
        '''

        status = False
        query = "select alwayson from lb_clusters"

        sqlite_handle = get_sqlite_handle(LB_SQLITE_FILE % clusterid)
        if sqlite_handle:
            db_cursor = sqlite_handle.cursor()
            retry = 0
            while retry < MAX_RETRY:
                try:
                    db_cursor.execute(query)
                    row = db_cursor.fetchone()
                    if row:
                        status = True if int(row['alwayson']) else False
                    break
                except Exception, ex:
                    retry = retry + 1
                    if retry >= MAX_RETRY:
                        _logger.error("Failed to read always_on status of" \
                                      " clusters: %s" % ex)
                    else:
                        time.sleep(0.1)

            close_sqlite_resources(sqlite_handle, db_cursor)
        return status

    def _cleanup_marker_files(self):
        '''
        Remove all marker files in use by service or its children
        '''
        fl = glob.glob('/var/run/a_monitor*')
        for _file in fl:
            if os.path.exists(_file):
                os.remove(_file)

    def _stop_monitor_process_for_cluster(self, cid):
        '''
        Send a SIGTERM to the monitor process for cluster <cid> .
        '''
        phandle = gMonitoredClusters[cid]
        if phandle.is_alive():
            _logger.info("Parent: Cluster %d is marked down" % cid)
            _logger.info("Parent: Stopping monitor process for " \
                         "cluster: %d" % int(cid))
            phandle.terminate()
            #
            # After issuing SIGTERM we wait for 60 seconds at max for this process
            # to join. If it does not then we send a SIGKILL and remove its
            # marker file.
            # A possible race condition can reach here. Consider a case where
            # after SIGTERM child has removed its marker file but has not yet
            # joined in. 
            phandle.join(TIME_TO_WAIT_FOR_CHILD_JOIN)
            # check if process is still alive
            try:
                os.kill(phandle.pid, 0)
                # if still here this process is taking too much time, we kill it
                _logger.warn("Parent: Monitor process for cluster:" \
                             "%d is taking too long (> %d seconds) to quit, " \
                             "killing it now " % (cid, TIME_TO_WAIT_FOR_CHILD_JOIN))
                os.kill(phandle.pid, 9)

                # now join it so as to collect resources
                phandle.join()
            except Exception, ex:
                # process has stopped
                pass
            _logger.info("Parent: Successfully Stopped monitor " \
                         "process for cluster: %d" % int(cid))

    def _signal_handler(self, signum, frame):
        '''
        Process in the event of a signal that we received. Since this part
        belongs to parent, in the event a signal, we will make sure that
        we cleanup our children and then only exit.
        '''
        _logger.info("Parent: Got signal, prepairing to exit gracefully.")
        plist = []
        plist = multiprocessing.active_children() # also cleanup any dead children
        if len(plist) > 0:
            _logger.info("Parent: Found %d monitor children. Sending" \
                         " termination signal. " % (len(plist), ))

            for phandle in plist:
                if phandle.is_alive():
                    #
                    # p.terminate() issues SIGTERM to the child.
                    #
                    _logger.info("Parent: Terminating the process whose id: %d" % int(phandle.pid))
                    phandle.terminate()
            for phandle in plist:
                if phandle.is_alive():
                    phandle.join(TIME_TO_WAIT_FOR_CHILD_JOIN)

                    try:
                        os.kill(phandle.pid, 0)
                        # if still here this process is taking too much time, we kill it
                        os.kill(phandle.pid, 9)
                        # now join it so as to collect resources
                        phandle.join()
                    except Exception, ex:
                        # process has stopped
                        pass

        self._cleanup_marker_files()
        _logger.info("Monitor: Finished cleaning up.")

        # now we exit. since pid file is cleanedup by the calling instance's call
        # of stop() method, we donot have anything to cleanup as such.
        sys.exit()

    def _register_signal_handler(self):
        '''
        Registers a set of signals to catch.
        '''
        signals = [signal.SIGTERM, signal.SIGHUP]
        for s in signals:
            signal.signal(s, self._signal_handler)

    def spwan_monitor_children(self):
        running_cluster_ids, stopped_cluster_ids = self.get_list_of_cluster_ids()
        _logger.debug("Parent: Active clusters :%s, Stopped Clusters :%s"\
                        % (running_cluster_ids, stopped_cluster_ids))

        for cid in stopped_cluster_ids:
            marker_file = "/var/run/monitor_%d.file" % cid
            if os.path.exists(marker_file):
                self._stop_monitor_process_for_cluster(cid)

        running_cluster_ids = [cid for cid in \
                running_cluster_ids if self._read_status(cid)]

        for cid in running_cluster_ids:
            marker_file = "/var/run/monitor_%d.file" % cid
            if os.path.exists(marker_file) == False:
                _logger.info("Parent: Spawning a new monitor " \
                              "process for cluster: %d" % cid)
                p = multiprocessing.Process(target=\
                                            monitor_routine, \
                                            args=(cid, os.getpid()))
                p.start()
                gMonitoredClusters[cid] = p
            else:
                marker_pid = read_pid(marker_file)
                if marker_pid:
                    path = "/proc/%s" % marker_pid
                    check_path = os.path.exists(path)
                    if check_path == False:
                        try:
                            os.remove(marker_file)
                        except:
                            _logger.error("Parent: Error on deleting marker file")
    
    def run(self):
        try:
            self._register_signal_handler()
        except Exception, ex:
            _logger.error("Parent: Failed to install signal handler: %s" % ex)
            _logger.error("%s" % (traceback.format_exc(),))
            sys.exit()

        while not os.path.exists(CWD+'/'+'lb.sqlite'):
            _logger.warn("Parent(%d): 'lb.sqlite' "\
                            "does not exist " % (os.getpid(),))
            time.sleep(1)

        while True:
            try:
                # cleanup any finished children
                multiprocessing.active_children()
                #
                # see if a new cluster has been added and that we need any
                # monitor process for it.
                #
                self.spwan_monitor_children()
                
                if not os.path.exists("/var/run/monitor.pid"):
                    _logger.warn("Monitor PID file is not Present Exiting Now")
                    break

            except Exception, ex:
                _logger.error("MonitorDaemon run failed: %s" % ex)
                _logger.error("%s" % (traceback.format_exc(),))
            finally:
                _logger.debug("Parent: Sleeping for %f seconds" \
                              % (SLEEP_INTERVAL))
                time.sleep(SLEEP_INTERVAL)
def main():
    '''
    Can only be execute as root, since we need to log in /var/run
    this can be changed later on 
    '''
    # Go away if you are not root
    if not os.geteuid() == 0:
        sys.exit("monitor: You must be root to run this script\n")

    # Parse the command line options
    try:
        opts, args = getopt.getopt(sys.argv[1:], \
                            'hdv', \
                            ["help", "debug", "version"])
    except:
        _usage("error parsing options")
    for opt in opts:
        if opt[0] == '-v' or opt[0] == '--version':
            print "%s: version %s" % (os.path.basename(sys.argv[0]), \
                                      SCRIPT_VERSION)
            sys.exit(0)
        elif opt[0] == '-h' or opt[0] == '--help':
            _usage()
        elif opt[0] == '-d' or opt[0] == '--debug':
            global _debug
            _debug = True
    if len(args) > 2:
        _usage('Invalid args %s %d' % args, len(args))

    # Initialize the logger
    log.config_logging()
    global _config
    _config = get_config_parser(MONITOR_CONF)
    
    monitor_daemon = MonitorDaemon('/var/run/monitor.pid')
    if args:
        if 'stop' == args[0]:
            _logger.info("****************** Monitor stopping ********************")
            monitor_daemon.stop()
        elif 'restart' == args[0]:
            _logger.info("***************** Monitor restarting *******************")
            monitor_daemon.restart()
        else:
            err_msg = "Invalid command %s" % (repr(args[0]),)
            print >> sys.stderr, err_msg
            _logger.error("%s" % (err_msg,))
            sys.exit(2)
        sys.exit(0)

    if _debug:
        _logger.info("************ Monitor starting (debug mode)**************")
        monitor_daemon.foreground()
    else:
        _logger.info("****************** Monitor starting ********************")
        monitor_daemon.start()

if __name__ == "__main__":
    main()
