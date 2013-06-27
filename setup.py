from distutils.core import setup

setup(
    name='MPILogger',
    version='0.1',
    author='J. Richard Shaw',
    author_email='jrichardshaw@gmail.com',

    packages=['mpilogger'],
    license='LICENSE.txt',
    long_description=open('README.txt').read(),

    install_requires=['mpi4py'],
)