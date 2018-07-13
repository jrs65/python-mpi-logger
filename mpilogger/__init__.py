
import sys
import time
import logging
import array
import atexit
from distutils.version import LooseVersion

import mpi4py
from mpi4py import MPI


# Maximum length of message in characters
_message_maxlen = 2048

# Interval between checking for new logging messages.
# Used instead of Waitsome to reduce CPU load.
_sleep_interval = 0.01


# Because of clashes between `mpi4py` and `logging` when python is exiting we
# need to destroy the connections to the logging proceses at the module level.
# Object level destructors, or the `Handler.close` method will not work.
@atexit.register
def _destroy_log_comm():
    for lc in _log_comm_list:
        if lc.rank == 0:
            lc.Isend([None, MPI.INT], dest=0, tag=1)
        lc.Disconnect()

# Internal variable for keeping track of the log communicators.
_log_comm_list = []



class MPILogHandler(logging.Handler):
    """A Handler which logs messages over MPI to a single process
    which then write them to a file.

    This uses MPI-2's Dynamic Process Management to spawn a child process
    which listens for log messages and then writes them to the specified file.
    It checks for new messages every 0.01s.

    Note
    ----
    This Handler also makes `rank` and `size` available in the `logger.Record`
    for any formatter to use.

    """

    def __init__(self, logfile, comm=None, *args, **kwargs):
        """Create the logger.

        Parameters
        ----------
        logfile : string
            Name of file to write.
        comm : MPI.Intracomm
            MPI communicator used by this logger.
        """

        super(MPILogHandler, self).__init__(*args, **kwargs)

        self._logfile = logfile

        self._comm  = MPI.COMM_WORLD if comm is None else comm

        # Spawn new process for logging
        self._log_comm = self._comm.Spawn(sys.executable, args=[__file__, self._logfile])

        # Add the communicator to the list of ones to keep track of.
        _log_comm_list.append(self._log_comm)


    def __del__(self):
        # Note this does not get called unless the Handler has been removed
        # from the logger.

        _log_comm_list.remove(self._log_comm)

        if self._log_comm.rank == 0:
            self._log_comm.Isend([None, MPI.INT], dest=0, tag=1)
        self._log_comm.Disconnect()


    def emit(self, record):
        """Emit the log message.

        Parameters
        ----------
        record : logging.Record
            logging record that will get written to disk.
        """

        try:

            record.rank = self._comm.rank
            record.size = self._comm.size            

            msg = self.format(record)

            # If message too long, truncate it
            if len(msg) > _message_maxlen:
                msg = msg[:_message_maxlen]

            msg_buf = array.array('c', msg)

            # Send message to the logging process
            self._request = self._log_comm.Issend([msg_buf, MPI.CHAR], dest=0, tag=0)
            s = MPI.Status()
            self._request.Wait(status=s)


        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)
        


if __name__ == '__main__':

    if len(sys.argv) < 2:
        raise Exception("Too few arguments to MPI logging process.")

    # Get the logfile and tag from the command line arguments
    logfile = sys.argv[1]

    # Open the logfile
    fh = open(logfile, 'w')

    # Get the parent Intracomm
    comm_parent = MPI.Comm.Get_parent()

    # Initialise all the buffers to receive logging messages
    buffers = [(array.array('c', '\0') * _message_maxlen) for pi in range(comm_parent.remote_size)]
    requests = []

    # Create a request for checking if we should exit
    exit_request = comm_parent.Irecv([None, MPI.INT], source=0, tag=1)

    # Establish all the initial connections for receiving messages
    for pi in range(comm_parent.remote_size):
        request = comm_parent.Irecv([buffers[pi], MPI.CHAR], source=pi, tag=0)
        requests.append(request)

    while True:

        # Wait until any connection receives a message
        status_list = []
        ind_requests = MPI.Request.Testsome(requests, statuses=status_list)
        # Request.Waitsome() and Request.Testsome() return None or list from mpi4py 2.0.0
        if LooseVersion(mpi4py.__version__) >= LooseVersion('2.0.0'):
            num_requests = len(ind_requests)
        # older version of mpi4py
        else:
            num_requests, ind_requests = ind_requests

        # If a request has changed
        if num_requests > 0:

            # Iterate over changed requests and process them
            for ind, s in zip(ind_requests, status_list):

                # Check to see if there was an error.
                if s.Get_error() != 0:
                    raise Exception("Logging error (code %i)." % s.Get_error())
            
                # Write the message to disk
                msg_rank = s.Get_source()
                msg = buffers[msg_rank].tostring().rstrip('\0')
                fh.write('%s\n' % msg)
                fh.flush()

                # Replace the buffer and connection
                buffers[ind] = (array.array('c', '\0') * _message_maxlen)
                requests[ind] = comm_parent.Irecv([buffers[ind], MPI.CHAR], source=msg_rank, tag=0)
           
        if MPI.Request.Test(exit_request):
            # We should exit from this process.
            break

        time.sleep(_sleep_interval)


    comm_parent.Disconnect()
    fh.close()
