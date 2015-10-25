com.yahoo.ycsb.db.libcom_yahoo_ycsb_db_JAnanas.so : com/yahoo/ycsb/db/com_yahoo_ycsb_db_JAnanas.h com.yahoo.ycsb.db.com_yahoo_ycsb_db_JAnanas.o
	g++  -I/usr/lib64/jvm/java/include/\
	    -I/usr/lib64/jvm/java/include/linux/ -L/home/runhui/ananas/lib/.libs\
	        -fPIC -o com/yahoo/ycsb/db/libcom_yahoo_ycsb_db_JAnanas.so -shared\
		    -Wl,-soname,com/yahoo/ycsb/db/com_yahoo_ycsb_db_JAnanas.so com/yahoo/ycsb/db/com_yahoo_ycsb_db_JAnanas.o\
		        -lkvf
	javah -classpath . -d ./com/yahoo/ycsb/db/ com.yahoo.ycsb.db.JAnanas
com/yahoo/ycsb/db/com_yahoo_ycsb_db_JAnanas.h : com/yahoo/ycsb/db/JAnanas.class
	        javah -classpath . -d ./com/yahoo/ycsb/db/ com.yahoo.ycsb.db.JAnanas
com.yahoo.ycsb.db.com_yahoo_ycsb_db_JAnanas.o : com/yahoo/ycsb/db/JAnanas.cc
	g++ -c -I/usr/lib64/jvm/java/include/\
     -I/usr/lib64/jvm/java/include/linux/ -L/home/runhui/ananas/lib/.libs\
         -fPIC -o com/yahoo/ycsb/db/com_yahoo_ycsb_db_JAnanas.o com/yahoo/ycsb/db/JAnanas.cc -lkvf

com/yahoo/ycsb/db/JAnanas.class : com/yahoo/ycsb/db/JAnanas.java
	javac com/yahoo/ycsb/db/JAnanas.java

clean : 
	rm com/yahoo/ycsb/db/JAnanas.class
	rm com/yahoo/ycsb/db/*.h
run : 
	java -Djava.library.path=/home/runhui/ananas/java/src/com/yahoo/ycsb/db/\
	       	com.yahoo.ycsb.db.JAnanas

