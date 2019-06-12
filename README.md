# ArgoDSM

[ArgoDSM](https://www.it.uu.se/research/project/argo) is a software distributed
shared memory system which aims to provide great performance with a simplified
programming model. It is currently being developed in Uppsala University.

### Build requirements

* CMake, C11 and C++11 compliant compilers
* Intel MPI library and compiler
* libnuma
* doxygen (if building documentation)

### Build and install
The following steps will fetch and build ArgoDSM. It is important to **check out the nanos6 branch before building**. If one wishes to skip building ArgoDSM tests, line 4-8 in the example below may be omitted, and `-DARGO_TESTS=OFF` set as CMake parameter. If one wishes to skip generating ArgoDSM documentation, this can be done by setting `-DBUILD_DOCUMENTATION=OFF` as CMake parameter.
```sh
git clone https://gitlab.itwm.fraunhofer.de/EPEEC/argodsm.git
git checkout nanos6
cd argodsm
cd tests
wget https://github.com/google/googletest/archive/release-1.7.0.zip
unzip release-1.7.0.zip
mv googletest-release-1.7.0 gtest-1.7.0
cd ..
mkdir build
cd build
cmake -DARGO_BACKEND_MPI=ON                         \
      -DARGO_BACKEND_SINGLENODE=ON                  \
      -DARGO_TESTS=ON                               \
      -DBUILD_DOCUMENTATION=ON                      \
      -DCMAKE_CXX_COMPILER=mpicxx                   \
      -DCMAKE_C_COMPILER=mpicc                      \
      -DCMAKE_INSTALL_PREFIX=[argodsm install path] \
      ../
make
make test
make install
```
Argo tests (`make test`) may fail if memory allocation is restricted. In the case of a cluster utilizing slurm, it may be necessary to run `make test` as a slurm batch job. Note that running `make test` is not required to complete the installation procedure.

### Running applications
For the singlenode backend, the executables can be run like any other normal executable.

For the MPI backend, they need to be run using the matching mpirun or mpiexec application. A minimal recommended command follows:
```sh
mpirun -n ${NNODES} ${EXECUTABLE}
```
`${NNODES} is the number of hardware nodes available and ${EXECUTABLE} is the application to run. Further configuration may be necessary depending on the target system.

For more information please visit our [website](https://www.argodsm.com).
There is also a [quickstart guide](https://etascale.github.io/argodsm/) (Please refer to the above instructions for building, installing and executing applications from the nanos6 branch) and a [tutorial](https://etascale.github.io/argodsm/tutorial.html).

Please contact us at [contact@argodsm.com](mailto:contact@argodsm.com)

