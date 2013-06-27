import logging

from mpi4py import MPI

import mpilogger

comm = MPI.COMM_WORLD

logger = logging.getLogger('mpitestlogger')

# Construct MPI Logger
mh = mpilogger.MPILogHandler('testfile.log')

# Construct an MPI formatter which prints out the rank and size
mpifmt = logging.Formatter(fmt='[rank %(rank)s/%(size)s] %(asctime)s : %(message)s')
mh.setFormatter(mpifmt)

# Add the logger
logger.addHandler(mh)

# Iterate through and log some messages
for i in range(2):
    logger.warning("Hello (times %i) from rank %i (of %i)" % (i+1, comm.rank, comm.size))

