==========
MPI Logger
==========

MPI Logger allows the use of the standard library logging module in a safe,
efficient way, when running highly parallel MPI tasks. It does this by
spawning a new MPI process which acts as a single writer to a specified log
file. All log messages emitted by the standard `logging` module are dispatched
to this process.


