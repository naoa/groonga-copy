CC = gcc
CFLAGS = -Wall -O2 -lgroonga -I/usr/include/groonga

all: groonga_copy

install:
	install groonga_copy /usr/local/bin


groonga_copy : src/groonga_copy.c
	$(CC) src/groonga_copy.c -o groonga_copy $(CFLAGS)

clean:
	rm -rf groonga_copy

