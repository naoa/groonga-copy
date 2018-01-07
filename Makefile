CC = gcc
CFLAGS = -Wall -O2 -lgroonga -I/usr/include/groonga

all: groonga-copy

install:
	install groonga-copy /usr/local/bin


groonga-copy : src/groonga-copy.c
	$(CC) src/groonga-copy.c -o groonga-copy $(CFLAGS)

clean:
	rm -rf groonga-copy

