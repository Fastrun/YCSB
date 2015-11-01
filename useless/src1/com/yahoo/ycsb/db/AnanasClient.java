package com.yahoo.ycsb.db;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.ArrayList;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.BufferUnderflowException;

import com.yahoo.ycsb.db.JAnanas;

public class AnanasClient extends DB {
	private JAnanas ananas;
	private HashMap<String, Long> tableIds;
	/// Success is always 0. 
    	public static final int OK = 0;

        /// YCSB interprets anything non-0 as an error, but doesn't interpret the
	/// specific value.
	public static final int ERROR = 1;
/**
     * Serialize the fields and values for a particular key into a byte[]
     * array to be written to RAMCloud. This method uses a hand-coded
     * serializer. It's about 1.2-3x as fast as when using Java's object
     * serializer.
     */
    private static byte[]
    serialize(HashMap<String, ByteIterator> values)
    {
        byte[][] kvArray = new byte[values.size() * 2][];

        int kvIndex = 0;
        int totalLength = 0;
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            totalLength += 8;   // fields denoting key length and value length

            byte[] keyBytes = entry.getKey().getBytes();
            kvArray[kvIndex++] = keyBytes;
            totalLength += keyBytes.length;

            byte[] valueBytes = entry.getValue().toString().getBytes();
            kvArray[kvIndex++] = valueBytes;
            totalLength += valueBytes.length;
        }
        ByteBuffer buf = ByteBuffer.allocate(totalLength);
        buf.order(ByteOrder.LITTLE_ENDIAN);

        kvIndex = 0;
        for (int i = 0; i < kvArray.length / 2; i++) {
            byte[] keyBytes = kvArray[kvIndex++];
            buf.putInt(keyBytes.length);
            buf.put(keyBytes);
            byte[] valueBytes = kvArray[kvIndex++];
            buf.putInt(valueBytes.length);
            buf.put(valueBytes);
        }

        return buf.array();
    }

    /**
     * Deserialize the fields and values stored in a RAMCloud object blob
     * into the given HashMap. This method uses a hand-coded deserializer.
     * It's about 3-5x as fast as when using Java's object deserializer.
     */
    private static void
    deserialize(byte[] bytes, HashMap<String, ByteIterator> into) throws DBException
    {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        buf.order(ByteOrder.LITTLE_ENDIAN);

        boolean workingOnKey = false;
        try {
            while (true) {
                int keyByteLength = buf.getInt();
                workingOnKey = true;
                String key = new String(bytes, buf.position(), keyByteLength);
                buf.position(buf.position() + keyByteLength);

                int valueByteLength = buf.getInt();
                String value = new String(bytes, buf.position(), valueByteLength);
                buf.position(buf.position() + valueByteLength);

                into.put(key, new StringByteIterator(value));
                workingOnKey = false;
            }
        } catch (BufferUnderflowException e) {
            // Done, hopefully.
            if (buf.remaining() != 0 || workingOnKey) {
                throw new DBException("deserialize: ByteBuffer not parsed " +
                    "properly! Had " + buf.remaining() + " bytes left over!");
            }
        }
    }

	public static final boolean debug = false;
	public void init() throws DBException {
		assert ananas == null;
		assert tableIds == null;
		Properties props = getProperties();
		String locator = props.getProperty(LOCATOR_PROPERTY);// define by myself later
		if (locator == null)
		        throw new DBException("Missing property " + LOCATOR_PROPERTY);
		//you can do deal with other command line parameters here
		if (props.getProperty(DEBUG_PROPERTY) != null)
		      	debug = true;
		if (debug)
		        System.err.println("AnanasClient connecting to " + locator + " ...");
			            
		//
		//initialize the Ananas
		ananas = new JAnanas(locator);
		tableIds = new HashMap<String, Long>();
	}

	public void cleanup() throws DBException {
		ananas.disconnect();
		ananas = null;
		tableIds = null;
	}

/**
       * Delete a record from the database.
       *
       * @param table The name of the table
       * @param key The record key of the record to delete.
       * @return Zero on success, a non-zero error code on error.
 */
	public int delete(String table, String key) {
		try {
			ananas.remove(getTableId(table), key);
		} catch (Exception e) {
			if (debug) 
				System.err.println("RamCloudClient delete threw: " + e);
			return ERROR;
		}
		return OK;
	}
	/**
	 * Insert a record in the database. Any field/value pairs in the specified
	 * values Hashmap will be written into the record with the specified record
	 * key.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to insert
	 * @param values A Hashmap of field/value pairs to insert in the record
	 * @return Zero on success, ano-zero error code on error
	 */
	@Override
	public int insert(String table, String key, HashMap<String, ByteIterator> values) {
		byte[] value = serialize(values);
		try {
		      ananas.write(getTableId(table), key, value);
	        } catch (Exception e) {
	                if (debug)
		                System.err.println("RamCloudClient insert threw: " + e);
		           return ERROR;
		}
		 return OK;
	}

	/**
     	 * Read a record from the database. Each field/value pair from the result
     	 * will be stored in a HashMap.
    	 *
     	 * @param table The name of the table
     	 * @param key The record key of the record to read.
     	 * @param fields The list of fields to read, or null for all of them
    	 * @param result A HashMap of field/value pairs for the result
     	 * @return Zero on success, a non-zero error code on error or "not found".
     	 */
    	@Override	
	public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
		try {
            		ananas.read(getTableId(table), key);
       	 	} catch (Exception e) {
            		if (debug)
                		System.err.println("RamCloudClient read threw: " + e);
            		return ERROR;
        	}	
		return OK;
	}

	/**
    	 * Perform a range scan for a set of records in the database. Each field/value
    	 * pair from the result will be stored in a HashMap.
    	 *
    	 * @param table The name of the table
    	 * @param startkey The record key of the first record to read.
    	 * @param recordcount The number of records to read
    	 * @param fields The list of fields to read, or null for all of them
    	 * @param result A Vector of HashMaps, where each HashMap is a set field/value
   	 *               pairs for one record
    	 * @return Zero on success, a non-zero error code on error.
    	 */
    	@Override
    	public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
            	System.println("Warning: ananas doesn't support range scans yet.");
        	return OK;
    	}
	/**
    	 * Update a record in the database. Any field/value pairs in the specified
    	 * values HashMap will be written into the record with the specified record
    	 * key, overwriting any existing values with the same field name.
    	 *
   	 * The YCSB documentation makes no explicit mention of this, but as far as
   	 * I can gather an update here is akin to a SQL update. That is, we are
    	 * expected to preserve the values that aren't being updated. For RAMCloud,
    	 * this means having to do a read-modify-write.
    	 *
    	 * @param table The name of the table
    	 * @param key The record key of the record to write.
    	 * @param values A HashMap of field/value pairs to update in the record
    	 * @return Zero on success, a non-zero error code on error.
    	 */
    	@Override
    	public int
    	update(String table, String key, HashMap<String, ByteIterator> values)
    	{
		byte[] value = serialize(values); 
  	     	ananas.remove(getTableId(table), key, table);
		// how to use values ?
		
        	return insert(table, key, value);
    }
}
