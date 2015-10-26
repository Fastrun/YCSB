com.yahoo.ycsb.db.libcom_yahoo_ycsb_db_JAnanas.so : com/yahoo/ycsb/db/com_yahoo_ycsb_db_JAnanas.h com.yahoo.ycsb.db.com_yahoo_ycsb_db_JAnanas.o
	gcc  -I/usr/lib/jvm/java/include/\
	    -I/usr/lib/jvm/java/include/linux/ -L/home/runhui/kvf_test/ananas-nvmkv/ananas-master/lib/.libs\
	        -fPIC -o com/yahoo/ycsb/db/libcom_yahoo_ycsb_db_JAnanas.so -shared\
		    -Wl,-soname,com/yahoo/ycsb/db/com_yahoo_ycsb_db_JAnanas.so com/yahoo/ycsb/db/com_yahoo_ycsb_db_JAnanas.o\
		        -lkvf
	javah -classpath . -d ./com/yahoo/ycsb/db/ com.yahoo.ycsb.db.JAnanas
com/yahoo/ycsb/db/com_yahoo_ycsb_db_JAnanas.h : com/yahoo/ycsb/db/JAnanas.class
	        javah -classpath . -d ./com/yahoo/ycsb/db/ com.yahoo.ycsb.db.JAnanas
com.yahoo.ycsb.db.com_yahoo_ycsb_db_JAnanas.o : com/yahoo/ycsb/db/JAnanas.c
	gcc -c -I/usr/lib/jvm/java/include/\
     -I/usr/lib/jvm/java/include/linux/ -L/home/runhui/kvf_test/ananas-nvmkv/ananas-master/lib/.libs\
         -fPIC -o com/yahoo/ycsb/db/com_yahoo_ycsb_db_JAnanas.o com/yahoo/ycsb/db/JAnanas.c -lkvf

com/yahoo/ycsb/db/JAnanas.class : com/yahoo/ycsb/db/JAnanas.java
	javac com/yahoo/ycsb/db/JAnanas.java

clean : 
	rm com/yahoo/ycsb/db/JAnanas.class
	rm com/yahoo/ycsb/db/c*.h
	rm com/yahoo/ycsb/db/*.so
run : 
	java -Djava.library.path=/home/runhui/ananas/java/src/com/yahoo/ycsb/db/\
	       	com.yahoo.ycsb.db.JAnanas

