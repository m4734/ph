test: test/test.o
	g++ -o bin/test test/test.o -lpthread

test/test.o: test/test.c test/test.h
	g++ -c -o test/test.o test/test.c

clean:
	rm -rf test/*.o bin/*
