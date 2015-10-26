package com.yahoo.ycsb.db;

/*
 * This class provides Java bindings for ananas. Right now it is a rather
 * simple subset of what Ananas.h defines.
 *
 * Running ``javah'' on this file will generate a C header file with the
 * appropriate JNI function definitions. The glue interfacing to the C++
 * Ananas library can be found in JAnanas.cc.
 *
 * For JNI information, the IBM tutorials and Android developer docs are much
 * better than Sun's at giving an overall intro:
 *      http://www.ibm.com/developerworks/java/tutorials/j-jni/section4.html
 *      http://developer.android.com/training/articles/perf-jni.html
 */
public class JAnanas {
    static {
        System.load("/home/runhui/ananas-ycsb/java/src/com/yahoo/ycsb/db/libcom_yahoo_ycsb_db_JAnanas.so"); //To load JAnanas
    }

    /// Pointer to the underlying C++ Ananas object associated with this
    /// object.
    private long AnanasObjectPointer = 0;

    /**
     * This class is returned by Read operations. It encapsulates the entire
     * object, including the key, value, and version.
     *
     * It mostly exists because Java doesn't support primitive out parameters
     * or multiple return values, and we don't know the object's size ahead of
     * time, so passing in a fixed-length array would be problematic.
     */
    public class Object {
        Object(byte[] _key, byte[] _value, long _version)
        {
            key = _key;
            value = _value;
            version = _version;
        }

        public String
        getKey()
        {
            return new String(key);
        }

        public String
        getValue()
        {
            return new String(value);
        }

        final public byte[] key;
        final public byte[] value;
        final public long version;
    }

    /**
     * Connect to the ananas cluster specified by the given coordinator's
     * service locator string. This causes the JNI code to instantiate the
     * underlying ananas C++ object.
     */
    public
    JAnanas(String coordinatorLocator)
    {
        AnanasObjectPointer = connect(coordinatorLocator);
    }

    /**
     * Disconnect from the ananas cluster. This causes the JNI code to
     * destroy the underlying ananas C++ object.
     */
    public void
    disconnect()
    {
        if (AnanasObjectPointer != 0) {
            disconnect(AnanasObjectPointer);
            AnanasObjectPointer = 0;
        }
    }

    /**
     * This method is called by the garbage collector before destroying the
     * object. The user really should have called disconnect, but in case
     * they did not, be sure to clean up after them.
     */
    public void
    finalize()
    {
        System.err.println("warning: Jananas::disconnect() was not called " +
                           "prior to the finalizer. You should disconnect " +
                           "your Jananas object when you're done with it.");
        disconnect();
    }

    /**
     * Convenience read() wrapper that take a String key argument.
     */
    public Object
    read(long tableId, String key)
    {
        return read(tableId, key.getBytes());
    }

    /**
     * Convenience read() wrapper that take a String key argument.
     */
/*    public Object
    read(long tableId, String key, RejectRules rules)
    {
        return read(tableId, key.getBytes(), rules);
    }
*/
    
    /**
     * Convenience remove() wrapper that take a String key argument.
     */
    public long
    remove(long tableId, String key)
    {
        return remove(tableId, key.getBytes());
    }

    /**
     * Convenience remove() wrapper that take a String key argument.
     */
/*    public long
    remove(long tableId, String key, RejectRules rules)
    {
        return remove(tableId, key.getBytes(), rules);
    }
*/
    /**
     * Convenience write() wrapper that take String key and value arguments.
     */
    public long
    write(long tableId, String key, String value)
    {
        return write(tableId, key.getBytes(), value.getBytes());
    }

    /**
     * Convenience write() wrapper that take String key and value arguments.
     */
/*    public long
    write(long tableId, String key, String value, RejectRules rules)
    {
        return write(tableId, key.getBytes(), value.getBytes(), rules);
    }
*/
    /**
     * Convenience write() wrapper that takes a String key and a byte[] value
     * argument.
     */
    public long
    write(long tableId, String key, byte[] value)
    {
        return write(tableId, key.getBytes(), value);
    }

    /**
     * Convenience write() wrapper that takes a String key and a byte[] value
     * argument.
     */
/*    public long
    write(long tableId, String key, byte[] value, RejectRules rules)
    {
        return write(tableId, key.getBytes(), value, rules);
    }
*/
    private static native long connect(String coordinatorLocator);
    private static native void disconnect(long ananasObjectPointer);

    public native long createTable(String name);
    public native long createTable(String name, int serverSpan);
    public native void dropTable(String name);
    public native long getTableId(String name);
    public native Object read(long tableId, byte[] key);
//    public native Object read(long tableId, byte[] key, RejectRules rules);
    public native long remove(long tableId, byte[] key);
//    public native long remove(long tableId, byte[] key, RejectRules rules);
    public native long write(long tableId, byte[] key, byte[] value);
//    public native long write(long tableId, byte[] key, byte[] value, RejectRules rules);

    /*
     * The following exceptions may be thrown by the JNI functions:
     */

    public class TableDoesntExistException extends Exception {
        public TableDoesntExistException(String message)
        {
            super(message);
        }
    }

    public class ObjectDoesntExistException extends Exception {
        public ObjectDoesntExistException(String message)
        {
            super(message);
        }
    }

    public class ObjectExistsException extends Exception {
        public ObjectExistsException(String message)
        {
            super(message);
        }
    }

    public class WrongVersionException extends Exception {
        public WrongVersionException(String message)
        {
            super(message);
        }
    }

    /**
     * A simple end-to-end test of the java bindings.
     */
    public static void
    main(String argv[])
    {
        JAnanas ananas = new JAnanas("hello");
        long tableId = ananas.createTable("hi");
        System.out.println("created table, id = " + tableId);
        long tableId2 = ananas.getTableId("hi");
        System.out.println("getTableId says tableId = " + tableId2);

        System.out.println("wrote obj version = " +
            ananas.write(tableId, "thisIsTheKey", "thisIsTheValue"));

        JAnanas.Object o = ananas.read(tableId, "thisIsTheKey");
        System.out.println("read object: key = [" + o.getKey() + "], value = ["
            + o.getValue() + "], version = " + o.version);

        ananas.remove(tableId, "thisIsTheKey");

        try {
            ananas.read(tableId, "thisIsTheKey");
            System.out.println("Error: shouldn't have read successfully!");
        } catch (Exception e) {
            // OK
        }

        ananas.write(tableId, "thisIsTheKey", "thisIsTheValue");
        long before = System.nanoTime();
        for (int i = 0; i < 100000; i++) {
            JAnanas.Object unused = ananas.read(tableId, "thisIsTheKey");
        }
        long after = System.nanoTime();
        System.out.println("Avg read latency: " +
            ((double)(after - before) / 100000 / 1000) + " usec");
    }
}

