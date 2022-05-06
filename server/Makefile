
server: main.o thread.o query.o data.o hash.o global.o
	g++ -o server -g main.o thread.o query.o data.o hash.o global.o -lpthread -lpmem

main.o: global.h thread.h data.h hash.h main.c
	g++ -c -g -o main.o main.c

thread.o: thread.h thread.c query.h global.h
	g++ -c -g -o thread.o thread.c

query.o: query.h data.h hash.h query.c
	g++ -c -g -o query.o query.c

data.o: data.h hash.h data.c global.h query.h
	g++ -c -g -o data.o data.c

hash.o: global.h hash.h hash.c
	g++ -c -g -o hash.o hash.c

global.o: global.h global.c data.h
	g++ -c -g -o global.o global.c

clean:
	rm *.o server
