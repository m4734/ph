
server: main.o thread.o query.o data.o hash.o global.o writeThread.o
	g++ -g -o server main.o thread.o query.o data.o hash.o global.o writeThread.o -lpthread -lpmem

main.o: global.h thread.h data.h hash.h main.c
	g++ -g -c -o main.o main.c

thread.o: thread.h thread.c global.h writeThread.h
	g++ -g -c -o thread.o thread.c

query.o: query.h data.h hash.h query.c
	g++ -g -c -o query.o query.c

data.o: data.h hash.h data.c global.h query.h
	g++ -g -c -o data.o data.c

hash.o: global.h hash.h hash.c
	g++ -g -c -o hash.o hash.c

global.o: global.h global.c data.h
	g++ -g -c -o global.o global.c

writeThread.o: writeThread.h writeThread.c connection.h
	g++ -g -c -o writeThread.o writeThread.c

clean:
	rm *.o server
