KVF= /home/runhui/kvf_test/ananas-nvmkv/ananas-master
LDFLAGS= -I$(KVF)/include/kvf -L$(KVF)/lib/.libs -lkvf

all: kvf-test
        LD_LIBRARY_PATH=$(KVF)/lib/.libs

kvf-test: kvf-test.h

clean:
	rm -f *.o kvf-test                         
