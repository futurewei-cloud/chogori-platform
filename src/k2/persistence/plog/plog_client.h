#pragma once

#include <stdint.h>
#include "plog_client_common.h"
namespace k2 {

/*
for example:
3az 20+16       az_cnt = 3, origin_num = 20 redundancy_num = 16;
single az 12+3  az_cnt = 1, origin_num = 12 redundancy_num = 3;
single az 3rep  az_cnt = 1, origin_num = 1  redundancy_num = 2;
3az rep 3       az_cnt = 3, origin_num = 1  redundancy_num = 2;
*/

// BAK_DB added
// For data backup
#define DEFAULT_PERF_TYPE_FOR_BACKUP       PL_PERF_TYPE_HDD
#define DEFAULT_PLOG_DURABILITY_FOR_BACKUP SINGLE_AZ_EC

typedef struct plog_durability_v2
{
    /* how many AZs for this plog layout  */
    uint8_t az_cnt;

    /* origin data num:
       For X replicas, X replicas are regarded as 1+M, set origin_num as 1.
       For EC N+M, set origin_num as N. */
    uint8_t origin_num;

    /* redundancy data num:
       For X replicas, X replicas are regarded as 1+M, set redundancy_num as M.
       For EC N+M set redundancy_num as M. */
    uint8_t redundancy_num;

    uint8_t reliability_level; /*the secury level of this durability type*/

    char    reserved[12];
}plog_durability_v2_t;


#define MAX_DURABILITY_VEC_SIZE  1024

typedef struct plog_durability_vec
{
    uint16_t                total_cnt;
    plog_durability_v2_t    plog_durability[MAX_DURABILITY_VEC_SIZE];    /* Data Redundancy Mode */
}plog_durability_vec_t;


/**
 * PLog descriptor V2:
 *    the user must use the descriptor to tell Persistence to create the plog with
 *    specified performance/durabilit_v2/capacity.
 */
typedef struct plog_descriptor_v2
{
    plog_durability_v2_t plog_durability_v2; /* Data Redundancy Mode        16Byte*/
    plog_perf_type_e     perf_type;          /* Performace type media type   4Byte*/
    plog_capacity_e      plog_capacity;      /* plog Capacity                4Byte*/
    affinity_policy_t    affinity_policy;    /* affinity attribute, just for SGL, 16Byte */
    cache_ctrl_flag_e    cache_priority;     /* cache priority, just for SGL, 4Byte*/
    char                 reserved[8];
}plog_descriptor_v2_t;

typedef struct plog_io_guide_info_v2
{
    /* origin data num:
       For X replicas, X replicas are regarded as 1+M, set origin_num as 1.
       For EC N+M, set origin_num as N. */
    uint8_t origin_num;

    //uint8_t min_origin_num; /* Minimum origin num supported */
    uint8_t  max_origin_num; /* Maximum origin num supported */

    uint32_t chunk_size; /* single block size , full stripe =  origin_num * block_size; */

    //uint32_t min_append_block_size; //not used any more.
    //uint32_t preferred_append_block_size;/* full stripe =  origin_num * chunk_size; */
    //uint32_t max_append_block_size;

    char     reserved[8];
}plog_io_guide_info_v2_t;

/**
 * struct of return value for the API to query PLOG.
 */
typedef struct plog_info_v2
{
    plog_descriptor_v2_t     desc;                 /* description */
    plog_state_info_t        state;                /* state info */
    plog_io_guide_info_v2_t  io_guide_info;        /* guide info */
    uint32_t                 real_capacity;        /* real capacity of the plog*/
    uint32_t                 plog_expired_time;    /* time of the plog expired that would be sealed*/
    char                     reserve[8];
}plog_info_v2_t;

/**
 * Plog V2:
 * A plog has an id and basic info.
 */
typedef struct plog_v2
{
    plog_id_t              id;                /* id */
    plog_info_v2_t         info_v2;              /* basic info of the plog */
}plog_v2_t;


/**
 * Plog vector:
 * currently, the plogs in one group will have the same info.
 */
typedef struct plog_vec_v2
{
    uint32_t                cnt;               /* plog cnt */
    plog_v2_t*              plogs;             /* plogs */
}plog_vec_v2_t;


/*******************************************************************************
* Function Description: create plog synchronously.
            Create group of plog with the same plog metadata.

* Input Parameters:
            @descriptor: meta data for creating plog
            @cnt: number of plog to create, maxmum: MAX_PLOG_NUM_PER_CREATION
            @trace_info: used for debugging
            @plog_expired_time: timeout value use default value when passing 0, default expire time is 30Days,��λΪmin
            @create_option:
            @timeout_in_ms: timeout
            @trace_info: used for debug

* Output Parameters:
            @plogs: vector of plogs that created

* Return Value:
            P_ARGS_INVAILD
            P_REQUEST_TIMEOUT
            P_PERF_AND_DUR_NOT_SUPPORTED
            P_CAPACITY_NOT_ENOUGH
            P_EXCEED_PLOGID_LIMIT
            P_BUSY
            P_OK
            P_ERROR
* Comments:
*******************************************************************************/
int create_plog(const plog_descriptor_t* descriptor, const uint32_t cnt, const uint32_t plog_expired_time, const create_option_t* create_option,
                const uint32_t timeout_in_ms, void* trace_info, plog_vec_t* plogs);



/*******************************************************************************
* Function Description: create plog synchronously extension interface.
            Create group of plog with the same plog metadata.

* Input Parameters:
            @descriptor: meta data for creating plog
            @cnt: number of plog to create, maxmum: MAX_PLOG_NUM_PER_CREATION
            @trace_info: used for debugging
            @plog_expired_time: timeout value use default value when passing 0, default expire time is 30Days,??��?min
            @create_option:
            @timeout_in_ms: timeout
            @trace_info: used for debug

* Output Parameters:
            @plogs: vector of plogs that created

* Return Value:
            P_ARGS_INVAILD
            P_REQUEST_TIMEOUT
            P_PERF_AND_DUR_NOT_SUPPORTED
            P_CAPACITY_NOT_ENOUGH
            P_EXCEED_PLOGID_LIMIT
            P_BUSY
            P_OK
            P_ERROR
* Comments:
*******************************************************************************/
int create_plog_v2(const plog_descriptor_v2_t* descriptor, const uint32_t cnt, const uint32_t plog_expired_time,
                       const create_option_t* create_option,  const uint32_t timeout_in_ms, void* trace_info, plog_vec_v2_t* plogs);


/*******************************************************************************
* Function Description: To creat plog asynchronously.
            Ceate group of plog with the same plog metadata.

* Input Parameters:
            @descriptor: meta data for creating plog
            @cnt: number of plog to create, maxmum: MAX_PLOG_NUM_PER_CREATION
            @trace_info:   16 bytes used for debugging
            @plog_expired_time: timeout value use default value when passing 0, default expire time is 30Days
            @create_option:
            @timeout_in_ms: timeout
            @trace_info: used for debug
            @cb: the callback

* Output Parameters:
            None

* Return Value:
            P_ARGS_INVAILD
            P_REQUEST_TIMEOUT
            P_PERF_AND_DUR_NOT_SUPPORTED
            P_CAPACITY_NOT_ENOUGH
            P_EXCEED_PLOGID_LIMIT
            P_BUSY
            P_OK
            P_ERROR
* Comments:
*******************************************************************************/
int create_plog_async(const plog_descriptor_t* descriptor, const uint32_t cnt, const uint32_t plog_expired_time, const create_option_t* create_option,
                      const uint32_t timeout_in_ms, void* trace_info, plog_vec_t* plogs, const plog_call_back_t* cb);

/*******************************************************************************
* Function Description: To query metadata of plog synchronously.

* Input Parameters:
            @plog_id:  PlogId
            @timeout_in_ms:
            @trace_info: used for debugging

* Output Parameters:
            @plog_info: plog_info_t

* Return Value:
            P_ARGS_INVAILD
            P_REQUEST_TIMEOUT
            P_PLOG_ID_NOT_EXIST
            P_PLOG_UNAVAILABLE
            P_PLOG_CRRUPTED
            P_BUSY
            P_OK
            P_ERROR

* Comments:
*******************************************************************************/
int get_plog_info(const plog_id_t* plog_id, const uint32_t timeout_in_ms, void* trace_info, plog_info_t* plog_info);



int get_plog_info_v2(const plog_id_t* plog_id, const uint32_t timeout_in_ms, void* trace_info, plog_info_v2_t* plog_info);



/*******************************************************************************
* Function Description: To query metadata of plog asynchronously.

* Input Parameters:
            @plog_id:  PlogId
            @timeout_in_us: timeout value use default value when passing 0
            @trace_info:
            @cb: callback function and context

* Output Parameters:
            @plog_info: plog_info_t

* Return Value:
            P_ARGS_INVAILD
            P_REQUEST_TIMEOUT
            P_PLOG_ID_NOT_EXIST
            P_PLOG_UNAVAILABLE
            P_PLOG_CRRUPTED
            P_BUSY
            P_OK
            P_ERROR
* Comments:
*******************************************************************************/
int get_plog_info_async(const plog_id_t* plog_id, const uint32_t timeout_in_ms, void* trace_info, plog_info_t* info, const plog_call_back_t* cb);

/*******************************************************************************
* Function Description: Seal plog synchronously.

* Input Parameters:
            @plog_id:  PlogId
            @timeout_in_us: timeout value use default value when passing 0
            @trace_info: used for debugging

* Output Parameters:
            None
* Return Value:
            P_ARGS_INVAILD
            P_REQUEST_TIMEOUT
            P_PLOG_ID_NOT_EXIST
            P_PLOG_IS_SEALLING
            P_PLOG_UNAVAILABLE
            P_PLOG_CRRUPTED
            P_PLOG_SEALED
            P_BUSY
            P_OK
            P_ERROR
* Comments:
*******************************************************************************/
int seal_plog(const plog_id_t* plog_id, const uint32_t timeout_in_ms, void* trace_info);

/*******************************************************************************
* Function Description: To seal plog asynchronously.

* Input Parameters:
            @plog_id:  PlogId
            @timeout_in_us: timeout value use default value when passing 0
            @trace_info:
            @cb: callback function and context

* Output Parameters:
            None

* Return Value:
            P_ARGS_INVAILD
            P_REQUEST_TIMEOUT
            P_PLOG_ID_NOT_EXIST
            P_PLOG_IS_SEALLING
            P_PLOG_UNAVAILABLE
            P_PLOG_CRRUPTED
            P_PLOG_SEALED
            P_BUSY
            P_OK
            P_ERROR
* Comments:
*******************************************************************************/
int seal_plog_async(const plog_id_t* plog_id, const uint32_t timeout_in_ms, void* trace_info, const plog_call_back_t* cb);

/*******************************************************************************
* Function Description: To append plog synchronously.
            1. Buffer size is limited to 2M when appending under MULTICOPY redundancy mode.
            2. For EC redundancy mode, limit is 2M* #of stripe.

* Input Parameters:
            @plog_id: PlogId
            @buffer_list: list of buffers, return the offset where stores first buffer
            @append_option: including priorities qos
            @timeout_in_us: timeout value use default value when passing 0
            @trace_info:   16 bytes used for debugging

* Output Parameters:
            @offset:    the offset where stores first buffer

* Return Value:
            P_ARGS_INVAILD
            P_PLOG_SEALED
            P_REQUEST_TIMEOUT
            P_PLOG_ID_NOT_EXIST
            P_CAPACITY_EXCEED
            P_PLOG_IS_SEALLING
            P_PLOG_UNAVAILABLE
            P_PLOG_CRRUPTED
            P_BUSY
            P_OK
            P_ERROR
* Comments:
*******************************************************************************/
int append_plog(const plog_id_t* plog_id, const plog_buffer_list_t* buffer_list, const append_option_t* append_option,
        const uint32_t timeout_in_ms, void* trace_info, uint64_t* offset);

/*******************************************************************************
* Function Description: To append plog asynchronously.

* Input Parameters:
            @plog_id: PlogId
            @buffer_list: list of buffers, return the offset where stores first buffer
            @append_option: including priorities qos
            @timeout_in_us: timeout value use default value when passing 0
            @trace_info:
            @cb: callback function and context

* Output Parameters:
            None

* Return Value:
            P_ARGS_INVAILD
            P_PLOG_SEALED
            P_REQUEST_TIMEOUT
            P_PLOG_ID_NOT_EXIST
            P_CAPACITY_EXCEED
            P_PLOG_IS_SEALLING
            P_PLOG_UNAVAILABLE
            P_PLOG_CRRUPTED
            P_BUSY
            P_OK
            P_ERROR
* Comments:
*******************************************************************************/
int append_plog_async(const plog_id_t* plog_id, const plog_buffer_list_t* buffer_list, const append_option_t* append_option,
        uint32_t timeout_in_ms, void* trace_info, uint64_t* offset, const plog_call_back_t* cb);

/*******************************************************************************
* Function Description: To read data from one plog.
            1. Support reading multiple positions within one plog which return the data into buffer lists.
            2. Buffer size is limited to 2M when reading data under MULTICOPY redundancy mode.
            3. For EC redundancy mode, limit is 2M* #of stripe.
            4. offset must be the start of crc, otherwise error will be returned.

* Input Parameters:
            @plog_id:  PlogId
            @data_v: Offset and length offset must be the start of crc, or return err
            @read_option:  same as AppendOption
            @timeout_in_us:    same as Append
            @trace_info:
            @buffer_list:  list of data buffers

* Output Parameters:
            @buffers:  list of data buffer

* Return Value:
            P_ARGS_INVAILD
            P_REQUEST_TIMEOUT
            P_PLOG_ID_NOT_EXIST
            P_PLOG_UNAVAILABLE
            P_PLOG_CRRUPTED
            P_BUSY
            P_OK
            P_ERROR
            P_CHECK_SUM_FAIL
* Comments:
*******************************************************************************/
int read_plog(const plog_id_t* plog_id, const plog_data_to_read_vec_t* data_v, const read_option_t* read_option, const uint32_t timeout_in_ms,
        void* trace_info, plog_buffer_list_t* buffer_list);

/*******************************************************************************
* Function Description: read_plog asynchronously
* Input Parameters:
            @plog_id:  PlogId
            @data_v: Offset and length offset must be the start of crc, or return err
            @read_option:  same as AppendOption
            @timeout_in_us:    same as Append
            @trace_info:
            @buffer_list:  list of data buffers
            @cb: callback function and context

* Output Parameters:
            None

* Return Value:
            P_ARGS_INVAILD
            P_REQUEST_TIMEOUT
            P_PLOG_ID_NOT_EXIST
            P_PLOG_UNAVAILABLE
            P_PLOG_CRRUPTED
            P_BUSY
            P_OK
            P_ERROR
            P_CHECK_SUM_FAIL
* Comments:
*******************************************************************************/
int read_plog_async(const plog_id_t* plog_id, const plog_data_to_read_vec_t* data_v, const read_option_t* read_option, const uint32_t timeout_in_ms,
        void* trace_info, plog_buffer_list_t* buffer_list, const plog_call_back_t* cb);

/*******************************************************************************
* Function Description: To delete the data and metada on Plog disks synchronously.
            1. if not return P_OK for deletion, Data in persistence layer could be fully deleted, partially deleted or not deleted at all Delete sealed plog only
            2. To ensure atomic, don't support delete group of plogs

* Input Parameters:
            @plog_id:  PLogId
            @timeout_in_us:    timeout value use default value when passing 0
            @trace_info:   used for debugging

* Output Parameters:
            None

* Return Value:
            P_ARGS_INVAILD
            P_REQUEST_TIMEOUT
            P_PLOG_UNSEALED
            P_PLOG_ID_NOT_EXIST
            P_PLOG_UNAVAILABLE
            P_PLOG_CRRUPTED
            P_BUSY
            P_OK
            P_ERROR
* Comments:
*******************************************************************************/
int delete_plog(const plog_id_t* plog_id, const uint32_t timeout_in_ms, void* trace_info);

/*******************************************************************************
* Function Description: delete_plog asynchronously
* Input Parameters:
            @plog_id:  PLogId
            @timeout_in_ms:   timeout value use default value when passing 0
            @trace_info:
            @cb: callback function and context

* Output Parameters:
            None

* Return Value:
            P_ARGS_INVAILD
            P_REQUEST_TIMEOUT
            P_PLOG_UNSEALED
            P_PLOG_ID_NOT_EXIST
            P_PLOG_UNAVAILABLE
            P_PLOG_CRRUPTED
            P_BUSY
            P_OK
            P_ERROR
* Comments:
*******************************************************************************/
int delete_plog_async(const plog_id_t* plog_id, const uint32_t timeout_in_ms, void* trace_info, const plog_call_back_t* cb);





/*******************************************************************************
* Function Description:
            query the system durability.
* Input Parameters:
            @type:            plog_perf_type_e
            @trace_info:   the trace id which used to trace the api call

* Output Parameters:
            @durability_vec   plog_durability_vec_t

* Return Value:
            P_ARGS_INVAILD
            P_REQUEST_TIMEOUT
            P_BUSY
            P_OK
            P_ERROR
            P_PERF_AND_DUR_NOT_SUPPORTED
* Comments:
*******************************************************************************/
int query_durability(const plog_perf_type_e type, void* trace_info, plog_durability_vec_t* durability_vec);

/* gc ���ʹ�øýӿ�,  ��ѯָ���������������ʣ������*/
int query_capacity_v2(const plog_perf_type_e type, const plog_durability_v2_t durability, void* trace_info, uint64_t* total_capacity, uint64_t* used_capacity);

} // ns k2
