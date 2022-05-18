
test: test/test.h test/test.c
	g++ -c -o test/test.o test/test.c
	g++ -o bin/test test/test.o -lpthread

clean:
	rm -rf test/*.o bin/* ph/*.o

ph: kvs_ph test/test.h test/test.c
	g++ -c -o test/test.o test/test.c -DPH
	g++ -o bin/test test/test.o ph/global.o ph/data.o ph/hash.o -lpthread

kvs_ph: ph/kvs.h ph/global.h ph/global.c ph/data.h ph/data.c ph/hash.h ph/hash.c
	g++ -c -o ph/data.o ph/data.c
	g++ -c -o ph/hash.o ph/hash.c
	g++ -c -o ph/global.o ph/global.c


debug:
	g++ -g -c -o test/test.o test/test.c
	g++ -g -o bin/test test/test.o -lpthread
