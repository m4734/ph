cd build
cmake -DBUILD_TARGET=PH
make
sudo numactl -N 0 -m 0 ./test
