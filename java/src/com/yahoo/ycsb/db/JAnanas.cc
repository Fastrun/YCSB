/* Copyright (c) 2015 CECA
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
extern "C" {
#include "../../../../../../include/kvf/kvf.h"
}
extern "C" {
#include "../../../../../../include/kvf/kvf_err_code.h"
}
extern "C" {
#include "../../../../../../include/kvf/kvf_api.h"
}

#include "com_yahoo_ycsb_db_JAnanas.h"


/// We will need this when using FindClass, etc.
#define PACKAGE_PATH "com/yahoo/ycsb/db/"

//don't care about exception now

#define check_null(var, msg)                                                \
    if (var == NULL) {       \
    }

/**
 * This class provides a simple means of extracting C-style strings
 * from a jstring and cleans up when the destructor is called. This
 * avoids having to manually do the annoying GetStringUTFChars /
 * ReleaseStringUTFChars dance. 
 */
class JStringGetter {
  private:
    JNIEnv* env;
    jstring jString;

  public:
    const char* astring;
  public:
    JStringGetter(JNIEnv* env, jstring jString)
        : env(env)
        , jString(jString)
        , astring(env->GetStringUTFChars(jString, 0))
    {
        check_null(astring, "GetStringUTFChars failed");
    }
/*why errors here?
    ~JStringGetter()
    {
        if (string != NULL)
            env->ReleaseStringUTFChars(jString, astring);
    }
*/

};

/**
 * This class provides a simple means of accessing jbyteArrays as
 * C-style void* buffers and cleans up when the destructor is called.
 * This avoids having to manually do the annoying GetByteArrayElements /
 * ReleaseByteArrayElements dance.
 */
class JByteArrayGetter {
  private:
    JNIEnv* env;
    jbyteArray jByteArray;

  public:
    void* const pointer;
    const jsize length;
  public:
    JByteArrayGetter(JNIEnv* env, jbyteArray jByteArray)
        : env(env)
        , jByteArray(jByteArray)
        , pointer(static_cast<void*>(env->GetByteArrayElements(jByteArray, 0)))
        , length(env->GetArrayLength(jByteArray))
    {
        check_null(pointer, "GetByteArrayElements failed");
    }

   /*
    ~JByteArrayGetter()
    {
        if (pointer != NULL) {
            env->ReleaseByteArrayElements(jByteArray,
                                          reinterpret_cast<jbyte*>(pointer),
                                          0);
        }
    }
*/

};
// the following method is not applicable
static kvf_type_t*
getKvf(JNIEnv* env, jobject jkvf_type_t)
{
    jclass cls = env->GetObjectClass(jkvf_type_t);// where to register?
    jfieldID fieldId = env->GetFieldID(cls, "ramcloudObjectPointer", "J");
    return reinterpret_cast<kvf_type_t*>(env->GetLongField(jkvf_type_t, fieldId));
}

static void
createException(JNIEnv* env, jobject jAnanas, const char* name)
{

}

/**
 * This macro is used to catch C++ exceptions and convert them into Java
 * exceptions. Be sure to wrap the individual RamCloud:: calls in try blocks,
 * rather than the entire methods, since doing so with functions that return
 * non-void is a bad idea with undefined(?) behaviour. 
 *
 * _returnValue is the value that should be returned from the JNI function
 * when an exception is caught and generated in Java. As far as I can tell,
 * the exception fires immediately upon returning from the JNI method. I
 * don't think anything else would make sense, but the JNI docs kind of
 * suck.
 */
#define EXCEPTION_CATCHER(_returnValue)
//    catch (TableDoesntExistException& e) {                                     \
//        createException(env, jRamCloud, "TableDoesntExistException");          \
//        return _returnValue;                                                   \
//    } catch (ObjectDoesntExistException& e) {                                  \
//        createException(env, jRamCloud, "ObjectDoesntExistException");         \
//        return _returnValue;                                                   \
//    } catch (ObjectExistsException& e) {                                       \
//        createException(env, jRamCloud, "ObjectExistsException");              \
//        return _returnValue;                                                   \
//    } catch (WrongVersionException& e) {                                       \
//        createException(env, jRamCloud, "WrongVersionException");              \
//        return _returnValue;                                                   \
//    }

/*
 * Class:     com_yahoo_ycsb_db_JAnanas
 * Method:    connect
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong
JNICALL Java_com_yahoo_ycsb_db_JAnanas_connect(JNIEnv *env,
                               jclass jkvf_tpye_t,
                               jstring coordinatorLocator)
{
    //JStringGetter locator(env, coordinatorLocator);// how is the second parameter used
    kvf_type_t* kvf = NULL;
    pool_t* pool = NULL;
    char * configfile; //
 //   try {
    	// 1.get kv-lib handle and init
    	kvf = get_kvf((char*)coordinatorLocator);
    	kvf_init(kvf, NULL);//where is the configure file parameter?? Ans: temporally NULL
    	// 2.create pool and open it.
    	pool_create("test", configfile, pool);
    	pool_open(pool);
    	int r = kvf_register(kvf);
    	if (r != RET_OK) {
    		//something go wrong

    	}
        //kvf = new kvf_type_t(locator.string);//??? kvf_type_t is not a class !
// XXX-- make this an option.
//kvf->clientContext->transportManager->setTimeout(10000);
      //  kvf->clientContext->transportManager->setSessionTimeout(10000);//???
//    } EXCEPTION_CATCHER(NULL);
    return reinterpret_cast<jlong>(kvf);
}

/*
 * Class:     com_yahoo_ycsb_db_JAnanas
 * Method:    disconnect
 * Signature: (J)V
 */
JNIEXPORT void
JNICALL Java_com_yahoo_ycsb_db_JAnanas_disconnect(JNIEnv *env,
                                  jclass jRamCloud,
                                  jlong ramcloudObjectPointer)
{
    delete reinterpret_cast<kvf_type_t*>(ramcloudObjectPointer);
}

/*
 * Class:     com_yahoo_ycsb_db_JAnanas
 * Method:    createTable
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jlong
JNICALL Java_com_yahoo_ycsb_db_JAnanas_createTable__Ljava_lang_String_2(JNIEnv *env,
                                                        jobject jRamCloud,
                                                        jstring jTableName)
{
    return Java_com_yahoo_ycsb_db_JAnanas_createTable__Ljava_lang_String_2I(env,
                                                            jRamCloud,
                                                            jTableName,
                                                            1);
}

/*
 * Class:     com_yahoo_ycsb_db_JAnanas
 * Method:    createTable
 * Signature: (Ljava/lang/String;I)I
 */
JNIEXPORT jlong
JNICALL Java_com_yahoo_ycsb_db_JAnanas_createTable__Ljava_lang_String_2I(JNIEnv *env,
                                                         jobject jRamCloud,
                                                         jstring jTableName,
                                                         jint jServerSpan)
{
    kvf_type_t* kvf = getKvf(env, jRamCloud);
    JStringGetter tableName(env, jTableName);
    unsigned long long tableId;
//    try {
//        tableId = kvf->createTable(tableName.string, jServerSpan);
//    } EXCEPTION_CATCHER(-1);
    return static_cast<jlong>(tableId);
}

/*
 * Class:     com_yahoo_ycsb_db_JAnanas
 * Method:    dropTable
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT void
JNICALL Java_com_yahoo_ycsb_db_JAnanas_dropTable(JNIEnv *env,
                                 jobject jRamCloud,
                                 jstring jTableName)
{
    kvf_type_t* kvf = getKvf(env, jRamCloud);
    JStringGetter tableName(env, jTableName);
//    try {
//        kvf->dropTable(tableName.string);
//    } EXCEPTION_CATCHER();
}

/*
 * Class:     com_yahoo_ycsb_db_JAnanas
 * Method:    getTableId
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong
JNICALL Java_com_yahoo_ycsb_db_JAnanas_getTableId(JNIEnv *env,
                                  jobject jRamCloud,
                                  jstring jTableName)
{
    kvf_type_t* kvf = getKvf(env, jRamCloud);
    JStringGetter tableName(env, jTableName);
    unsigned long long tableId;
//    try {
//        tableId = kvf->getTableId(tableName.string);
//    } EXCEPTION_CATCHER(-1);
    return tableId;
}

/*
 * Class:     com_yahoo_ycsb_db_JAnanas
 * Method:    read
 * Signature: (J[B)LJRamCloud/Object;
 */
JNIEXPORT jobject
JNICALL Java_com_yahoo_ycsb_db_JAnanas_read(JNIEnv *env,
                                  jobject jAnanas,
                                  jlong jPoolId,
                                  jbyteArray jKey)
{
    //kvf_type_t* kvf = getKvf(env, jAnanas);
    JByteArrayGetter key(env, jKey);

//    Buffer buffer;
    unsigned long long version;

    string_t value;
    string_t k;
    k.data = (s8*)key.pointer; // maybe problems here in cast
    k.len = key.length;
    pool_t* pool;// = getPool(env, jpool_t); // where to get this?
//    try {
        get(pool, &k, &value, NULL, NULL);
//    } EXCEPTION_CATCHER(NULL);

//    jbyteArray jValue = env->NewByteArray(buffer.getTotalLength());
//    jbyteArray jValue = env->NewByteArray(buffer.size());
//    check_null(jValue, "NewByteArray failed");
//    JByteArrayGetter value1(env, jValue);
    //buffer.copy(0, buffer.getTotalLength(), value.pointer);
//    buffer.copy(0, buffer.size(), value.pointer);

    // Note that using 'javap -s' on the class file will print out the method
    // signatures (the third argument to GetMethodID).
    jclass cls = env->FindClass(PACKAGE_PATH "JRamCloud$Object");
    check_null(cls, "FindClass failed");

    jmethodID methodId = env->GetMethodID(cls,
                                          "<init>",
                                          "(L" PACKAGE_PATH "JRamCloud;[B[BJ)V");
    check_null(methodId, "GetMethodID failed");

    return env->NewObject(cls,
                          methodId,
                          jAnanas,
                          jKey,
                          jKey,
                          static_cast<jlong>(version));
}



/*
 * Class:     com_yahoo_ycsb_db_JAnanas
 * Method:    remove
 * Signature: (J[B)J
 */
JNIEXPORT jlong
JNICALL Java_com_yahoo_ycsb_db_JAnanas_remove
  (JNIEnv *env, jobject JAnanas, jlong jpoolId, jbyteArray jKey)
{
    //kvf_type_t* kvf = NULL;
	pool_t* pool = NULL;

	//kvf = getKvf(env, jkvf_type_t);
	//pool = getPool(env, jpool_t);
	pool_open(pool);
	JByteArrayGetter key(env, jKey);
	string_t k;
	k.data = (s8*)key.pointer; // maybe problems here in cast
	k.len = key.length;

	del(pool, &k, NULL, NULL);
	pool_close(pool);


	// maybe use tableId?

    unsigned long long version;
//    try {
        //ramcloud->remove(jTableId, key.pointer, key.length, NULL, &version);

//    	del();
//    } EXCEPTION_CATCHER(-1);
    return static_cast<jlong>(version);
}



/*
 * Class:     com_yahoo_ycsb_db_JAnanas
 * Method:    write
 * Signature: (J[B[B)J
 */

JNIEXPORT jlong
JNICALL Java_com_yahoo_ycsb_db_JAnanas_write(JNIEnv *env,
                                      jobject jRamCloud,
                                      jlong jTableId,
                                      jbyteArray jKey,
                                      jbyteArray jValue)
{
    //kvf_type_t* kvf = getKvf(env, jRamCloud);
    JByteArrayGetter key(env, jKey);
    JByteArrayGetter value(env, jValue);
    unsigned long long version;	// is this useful?
	pool_t* pool;// = getPool(env, jPool);
	string_t akey, avalue;
	akey.data = (s8*)key.pointer;
	akey.len = key.length;
	avalue.data = (s8*)value.pointer;
//    try {
        put(pool, &akey, &avalue, NULL, NULL);
//    } EXCEPTION_CATCHER(-1);
    return static_cast<jlong>(version); // is version required?
}

int main() {
	return 0;
}
