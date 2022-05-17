test: test.o
	g++ -o test src/test.o -lpthread

test.o: test/test.c test/test.h
	g++ -o test/test.o test/test.c test/test.h
