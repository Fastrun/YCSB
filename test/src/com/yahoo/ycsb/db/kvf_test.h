#include "stdio.h"
#include "../../../../../../../KVF/ananas-master/include/kvf/kvf.h"
s32 nvmkv_pool_create(const char* name, const char* config_path, pool_t* pool){
        printf("create a pool\n");
        return RET_OK;
}

s32 nvmkv_pool_destroy(pool_t* pool){
        printf("destroy a pool\n");
        return RET_OK;
}

s32 nvmkv_kvlib_init(kvf_type_t* kvf, const char* config_file){
        printf("init a kvf\n");
        return RET_OK;
}

s32 nvmkv_kvlib_shutdown(kvf_type_t* kvf){
        printf("shutdown kvf\n");
        return RET_OK;
}

s32 nvmkv_kv_put(pool_t* pool, const string_t* key, const string_t* value, const kv_props_t* props, const put_options_t* putopts){
        printf("put a key-value\n");
        return 0;
}

s32 nvmkv_kv_get(pool_t* pool, const string_t* key, string_t* value, const kv_props_t* props, const get_options_t* getopts){
        printf("get a key-value\n");
        return 0;
}

s32 nvmkv_kv_del(pool_t* pool, const string_t* key, const kv_props_t* props, const del_options_t* delopts){
        printf("delete a key-value\n");
        return 0;
}

pool_operations_t nvmkv_pool_ops = {
        .create = nvmkv_pool_create,
        .destroy = nvmkv_pool_destroy,
        .open = NULL,
        .close = NULL,
        .set_prop = NULL,
        .get_prop = NULL,
        .get_stats = NULL
};

kvf_operations_t nvmkv_kvlib_ops = {
        .init = nvmkv_kvlib_init,
        .shutdown = nvmkv_kvlib_shutdown,
        .set_prop = NULL,
        .get_prop = NULL,
        .alloc_buf = NULL,
        .free_buf = NULL,
        .get_errstr = NULL,
        .get_stats = NULL,
        .trans_start = NULL,
        .trans_commit = NULL,
        .trans_abort = NULL
};

kv_operations_t nvmkv_kv_ops = {
        .put = nvmkv_kv_put,
        .get = nvmkv_kv_get,
        .del = nvmkv_kv_del,
        .mput = NULL,
        .mget = NULL,
        .mdel = NULL,
        .async_put = NULL,
        .async_update = NULL,
        .async_get = NULL,
        .async_del = NULL,
        .iter_open = NULL,
        .iter_next = NULL,
        .iter_close = NULL,
        .iter_pos_deserialize = NULL,
        .iter_pos_serialize = NULL,
        .xcopy = NULL
};

kvf_type_t nvmkv_kvlib_std = {
        .name = "nvm_kvlib",
        .kvf_ops = &nvmkv_kvlib_ops,
        .pool_ops = &nvmkv_pool_ops
};

pool_t nvmkv_pool_std = {
        .kv_ops = &nvmkv_kv_ops
};

