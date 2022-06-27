
test: test/test.h test/test.c
	g++ -c -o test/test.o test/test.c
	g++ -o bin/test test/test.o -lpthread

clean:
	rm -rf test/*.o bin/* ph/*.o viper/*.o

#ph: kvs_ph test/test.h test/test.c
ph: test/test.h test/test.c ph/kvs.h
	g++ -o bin/test test/test.c -DBUILD_PH ph/global.c ph/data.c ph/hash.c ph/query.c ph/thread.c ph/cceh.c ph/log.c -lpthread -lpmem -O3
#	g++ -c -o test/test.o test/test.c -DBUILD_PH -O3
#	g++ -o bin/test test/test.o ph/global.o ph/data.o ph/hash.o ph/query.o ph/thread.o ph/cceh.o -lpthread -lpmem -O3# -std=c++17	
	


kvs_ph: ph/kvs.h ph/global.h ph/global.c ph/data.h ph/data.c ph/hash.h ph/hash.c ph/thread.c ph/thread.h ph/cceh.h ph/cceh.c
	g++ -c -o ph/data.o ph/data.c -O3# -std=c++17
	g++ -c -o ph/hash.o ph/hash.c -O3# -std=c++17
	g++ -c -o ph/query.o ph/query.c -O3# -std=c++17
	g++ -c -o ph/global.o ph/global.c -O3# -std=c++17
	g++ -c -o ph/thread.o ph/thread.c -O3# -std=c++17
	g++ -c -o ph/cceh.o ph/cceh.c -O3# -std=c++17

viper: test/test.h test/test.c viper/kvs.h viper/viper.hpp viper/cceh.hpp viper/hash.hpp
	g++ -c -o test/test.o test/test.c -DBUILD_Viper -Iviper/concurrentqueue-src/ -std=c++17 -mclwb -O3
	g++ -o bin/test test/test.o -lpthread -O3

debug_viper: test/test.h test/test.c viper/kvs.h viper/viper.hpp viper/cceh.hpp viper/hash.hpp
	g++ -g -c -o test/test.o test/test.c -DBUILD_Viper -Iviper/concurrentqueue-src/ -std=c++17 -mclwb
	g++ -g -o bin/test test/test.o -lpthread


debug_ph: debug_kvs_ph test/test.h test/test.c
	g++ -g -c -o test/test.o test/test.c -DBUILD_PH
	g++ -g -o bin/test test/test.o ph/global.o ph/data.o ph/hash.o ph/query.o ph/thread.o ph/cceh.o ph/log.o -lpthread -lpmem

debug_kvs_ph: ph/kvs.h ph/global.h ph/global.c ph/data.h ph/data.c ph/hash.h ph/hash.c ph/thread.c ph/thread.h ph/cceh.h ph/cceh.c ph/log.c
	g++ -g -c -o ph/data.o ph/data.c
	g++ -g -c -o ph/hash.o ph/hash.c
	g++ -g -c -o ph/query.o ph/query.c
	g++ -g -c -o ph/global.o ph/global.c
	g++ -g -c -o ph/thread.o ph/thread.c 
	g++ -g -c -o ph/cceh.o ph/cceh.c
	g++ -g -c -o ph/log.o ph/log.c

debug_test:
	g++ -g -c -o test/test.o test/test.c
	g++ -g -o bin/test test/test.o -lpthread


.PHONY: test ph viper
