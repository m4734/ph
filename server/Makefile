server: main.o thread.o query.o data.o hash.o global.o
	g++ -o server main.o thread.o query.o data.o hash.o global.o -lpthread -lpmem

main.o: global.h thread.h data.h hash.h main.c
	g++ -c -o main.o main.c

thread.o: thread.h thread.c query.h
	g++ -c -o thread.o thread.c

query.o: query.h data.h hash.h query.c
	g++ -c -o query.o query.c

data.o: data.h hash.h data.c global.h
	g++ -c -o data.o data.c

hash.o: global.h hash.h hash.c
	g++ -c -o hash.o hash.c

global.o: global.h global.c
	g++ -c -o global.o global.c
