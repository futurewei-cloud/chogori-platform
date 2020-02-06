#pragma once

#include <stdint.h>
#include <k2/common/Common.h>

#define MAX_AFFINIY_SRV_ID (2)                           /* max affinity srv id */
namespace k2 {

struct PlogInfo {
    uint32_t size;
    bool sealed;
};

/**
*   just for SGL
*   affinity policy:
*   the user must use the policy to tell Persistence to create the plog with affinity.
*/
typedef struct affinity_policy
{
    uint8_t    cnt;                                         /* server total num */
    uint32_t   *server_id;                                 /* server id array point */
}affinity_policy_t;

/***
* Return Code definition Section
*/
/**
* just for SGL
* cache ctrl flag index:
* 1. 0:default
* 1. INDEX 03 for smarte cache write.
* 2. INDEX 4-7 for smarte cache read.
*
*/
typedef enum cache_ctrl_flag
{
    CACHE_PRIORITY_WRITE_READ_DEFAULT  = 0x0000,     /* read write read cache default. */
    CACHE_PRIORITY_WRITE_THROUGH_IDX   = 0x0002,     /* write cache write through idx */
    CACHE_PRIORITY_WRITE_RESIDENT_IDX  = 0x0004,     /* write cache write resident idx */
    CACHE_PRIORITY_READ_LOW_IDX        = 0x0010,     /* write cache read priority idx */
    CACHE_PRIORITY_READ_HIGH_IDX       = 0x0020,     /* write cache high priority idx */
}cache_ctrl_flag_e;



/***
 *     ####### Return Code definition Section #######
 */
typedef enum plog_ret_code
{
    P_BUSY                          = -100000,    /* PLog is busy. Please retry later. */
    P_ARGS_INVALID                  = -99999,     /* Invalid input args. */
    P_PLOG_SEALED                   = -99998,     /* The plog is sealed thus any append request would be rejected. */
    P_REQUEST_TIMEOUT               = -99997,     /* PLog request timeout. */
    P_PLOG_ID_NOT_EXIST             = -99996,     /* PLogID does not exist. */
    P_CAPACITY_NOT_ENOUGH           = -99995,     /* Insufficient PLog capacity. */
    P_CHECK_SUM_FAIL                = -99994,     /* Checksum fails. */
    P_EXCEED_PLOGID_LIMIT           = -99993,     /* Exceeds the limit of PLogIDs on single node. */
    P_PERF_AND_DUR_NOT_SUPPORTED    = -99992,     /* The PLog pool with specified perf type or durability level is not supported. */
    P_PLOG_CORRUPTED                = -99991,     /* The PLog has corrupted permanently and only parts of the data can be obtained. */
    P_PLOG_UNAVAILABLE              = -99990,     /* The PLog is unavailalbe. Recovery is not guranteed. */
    P_PLOG_IS_SEALLING              = -99989,     /* The PLog is sealing and cannot be accessed. Please retry later. */
    P_PLOG_UNSEALED                 = -99988,
    P_PLOG_CLIENT_NOT_READY         = -99987,     /* PLOG Client not ready yet */
    P_PLOG_CLIENT_UNAVAILABLE       = -99986,     /* canot find  PLOG Client service.  PLOG Client maybe down*/
    P_NAMESPACE_ID_INVALID          = -99985,     /* check namespaceid fail:namespaceid in req to plog server is different from namespaceid in vdb*/
    P_NAMESPACE_NOT_EXIST           = -99984,     /* namespace does not exist*/
    P_NAMESPACE_ALREADY_EXIST       = -99983,     /* namespace already exist*/

    P_ERROR                         = -1,         /* Unknown Error*/
    P_OK                            = 0,          /* OK */

}plog_ret_code_e;

/***
 *     ####### types definition Section #######
 */
// the max number of plog per one batch-creation
#define MAX_PLOG_NUM_PER_CREATION (256)

/***
 *     ####### Types definition Section #######
 */
/**
 **Plog Performance types
 * 1. currently support HDD, SSD and HDD with power saving.
 * 2. user only specifies plog perf type and plogmanager will allocate the plogs to meet the specification.
 *
 */
typedef enum plog_perf_type
{
    PL_PERF_TYPE_HDD = 0,                                /* Media type HDD */
    PL_PERF_TYPE_SSD = 1,                                /* Media type SSD */
    PL_PERF_TYPE_HDD_WITH_POWER_SAVING = 2,              /* Media type such as SMR, that used for cold storage and could do power saving */
    PL_PERF_TYPE_HDD_LOW = 3,
}plog_perf_type_e;

/**
 * Plog durability types:
 * 1. multi-copy supports 2/3/4 copies.
 * 2. erasure-coding supports different types as well. In order to choose EC type, user specifies the AZ level(single/two_az/tri_az) and
 *    plogmanager will dynamically choose which N+M to use based on comprehensive analysis of durability, performance and cost.
 */
typedef enum plog_durability
{
    // for single-AZ multi-copy:
    SINGLE_AZ_MULTICOPY_1  = 0,  //1              // copy, currently, not support
    SINGLE_AZ_MULTICOPY_2  = 1,  // copies.
    SINGLE_AZ_MULTICOPY_3  = 2,  // copies.
    SINGLE_AZ_MULTICOPY_4  = 3,  // copies.
    SINGLE_AZ_MULTICOPY_6  = 5,

    // for two-AZ multi-copy.
    TWO_AZ_MULTICOPY_2  = 11,
    TWO_AZ_MULTICOPY_3  = 12,
    TWO_AZ_MULTICOPY_4  = 13,

    // for three-AZ multi-copy.
    TRI_AZ_MULTICOPY_3  = 21,
    TRI_AZ_MULTICOPY_4  = 22,
    TRI_AZ_MULTICOPY_5  = 23,
    TRI_AZ_MULTICOPY_6  = 24,

    // for erasure-codings.
    SINGLE_AZ_EC = 50,
    TWO_AZ_EC    = 51,
    TRI_AZ_EC    = 52,
    SINGLE_AZ_EC_12_3 = 53,
    SINGLE_AZ_EC_6_3  = 54,
    TWO_AZ_EC_MIRROR_10_14  = 55,
    TWO_AZ_EC_MIRROR_5_9    = 56,
    TRI_AZ_EC_10_11    = 57,
    TRI_AZ_EC_20_16   = 58,

    // INTERNAL TEST ONLY. DO NOT USE FOR PRODUCTION.
    TEST_ONLY_EC_4_2   = 101,
    TEST_ONLY_EC_6_3   = 102,
    TEST_ONLY_EC_8_2   = 103,
    TEST_ONLY_EC_12_3  = 104,
    TEST_ONLY_EC_18_3  = 105,
    TEST_ONLY_EC_18_4  = 106,
    TEST_ONLY_EC_20_16 = 201,    //For multi-az
}plog_durability_e;

/**
 * PLog Capacity:
 *   Specified numbers are just suggestions, while the real size of a plog will depend on the media capacity.
 *     Taking SSD as the storage media, if user requests 64MB plog size while the block size of SSD is 96MB,
 *     Persistence would allocate 96MB plog for this request. User may gain performance benefit when the SSD
 *   can erase the entire block when delete the plog.
 */
typedef enum plog_capacity
{
    PLOG_SIZE_64M  = 0,
    PLOG_SIZE_128M = 1,
    PLOG_SIZE_256M = 2,
    PLOG_SIZE_512M = 3,
    PLOG_SIZE_1G   = 4,
    PLOG_SIZE_4G   = 5,
}plog_capacity_e;

/**
 * PLog descriptor:
 *   the user must use the descriptor to tell Persistence to create the plog with specified performance/durability/capacity.
 */
typedef struct plog_descriptor
{
    plog_perf_type_e     perf_type;          /* Performace type media type */
    plog_durability_e    plog_durability;    /* Data Redundancy Mode */
    plog_capacity_e      plog_capacity;      /* plog Capacity */
}plog_descriptor_t;

/**
 * Plog io guide info:
 *   the folowing table defines the block size of append operations in Persistence.
 *   Multi-copy supports byte level append, while erasure-coding only support full stripe(512K*N) level append now.
 * !!!Note
 *   For erasue-coding, user can only prepare the buffer with min_append_block_size.
 *   Persistence will process the buffer(split to N chunks and do erasure coding internal).
    ------------------------------------------------------------------------------------------------------
    |            filed                |    multi-copy(ssd)                            |erasure coding(HDD,N+M) |
    ------------------------------------------------------------------------------------------------------
    |    min_append_block_size        |    1B(fixed size)                            |        512K*N             |
    ------------------------------------------------------------------------------------------------------
    |    preferred_append_block_size  |    4K(variable size,decided by device type)  |        512K*N             |
    ------------------------------------------------------------------------------------------------------
    |    max_append_block_size        |    2M(fixed size)                            |        512K*N             |
    ------------------------------------------------------------------------------------------------------
 */
typedef struct plog_io_guide_info
{
    //uint32_t min_append_block_size; //not used any more.
    uint32_t preferred_append_block_size;
    uint32_t max_append_block_size;
}plog_io_guide_info_t;

/**
 * PLog State:
 * Stable state of one plog, and the state changes as:

 PLOG_UNSEAL -------> PLOG_NORMAL_SEALED
        |
        |--->PLOG_ABNORMAL_SEALED

    the state change is non-reversible and the executable io operation must follow this table:

    ---------------------------------------------------------------------------
    |    state\operation            | append |    read | delete | seal | getploginfo |
    ---------------------------------------------------------------------------
    |    PLOG_UNSEAL                |    Y     |     Y     |     N      |     Y     |      Y            |
    ---------------------------------------------------------------------------
    |    PLOG_NORMAL_SEALED        |    N    |     Y     |     Y      |     N     |      Y            |
    ---------------------------------------------------------------------------
    |    PLOG_ABNORMAL_SEALED    |    N    |     Y     |     Y      |     N     |      Y            |
    ---------------------------------------------------------------------------
 */
typedef enum plog_state
{
    PLOG_UNSEAL           = 0,                      /* normal for write */
    PLOG_ABNORMAL_SEALED  = 1,                      /* abnormal seal. for example: seal when layer Persistence Layer failed */
    PLOG_NORMAL_SEALED    = 2,                      /* normal sealed */
    PLOG_SEALING          = 3,                       /* plog is in progress of sealing, can not append and query will not give consistent result */
}plog_state_e;

/**
 * PLog state:
 * used by get_plog_info.
 */
typedef struct plog_state_info
{
    plog_state_e     state;                        /* plog state. */
    plog_ret_code_e  err_code_when_seal;           /* error code when seal. */
    uint64_t         last_known_valid_size;        /* plog's last known valid tail size. */
}plog_state_info_t;

/**
 * PLog ID:
 * A plog is a append-only data store unit, and each plog has an id which is unique in the entire system.
 */
#pragma pack(push)
#pragma pack(1)

#define PLOG_ID_LEN     24                         /* plog id len by bytes */

typedef struct plog_id
{
    char id[PLOG_ID_LEN];                          /* internal id in Persistence Layer */
}plog_id_t;

#pragma pack(pop)

/**
 * struct of return value for the API to query PLOG async.
 */
typedef struct plog_info
{
    plog_descriptor_t        desc;                 /* description */
    plog_state_info_t        state;                /* state info */
    plog_io_guide_info_t     io_guide_info;        /* guide info */
    uint32_t                 real_capacity;        /* real capacity of the plog*/
    uint32_t                 plog_expired_time;    /* time of the plog expired that would be sealed*/
}plog_info_t;

/**
 * Plog:
 * A plog has an id and basic info.
 */
typedef struct plog
{
    plog_id_t                 id;                /* id */
    plog_info_t               info;              /* basic info of the plog */
}plog_t;

/**
 * Plog vector:
 * currently, the plogs in one group will have the same info.
 */
typedef struct plog_vec
{
    uint32_t                cnt;               /* plog cnt */
    plog_t*                 plogs;             /* plogs */
}plog_vec_t;

/**
 * PLog io priority:
 * currently, we only support 3 levels priority.
 */
typedef enum plog_io_priority
{
    PLOG_HIGH_PRIORITY      = 0,
    PLOG_NORMAL_PRIORITY    = 1,
    PLOG_LOW_PRIORITY       = 2,
}plog_io_priority_e;

/**
 * PLog IO flag:
 * each flag can be used to control the behaviour of Persistence.
 */
typedef enum plog_create_flag
{
    PLOG_CREATE_DEFAULT_FLAG             = 0x0000,   /* default create */
    PLOG_CREATE_EMERGENCY_FLAG           = 0x0002,   /* emergency create */
    PLOG_CREATE_EXACT_FLAG               = 0x0004,   /* If set as 1, means persistence must create plog with specifed durability, not use */
    PLOG_CREATE_SMALL_WRITE              = 0x0008,   /* EC small write flag */
    //only for persistence layer test. do not use!!!
    PLOG_CREATE_TEST_FLAG                = 0x8000,
}plog_create_flag_e;

typedef enum plog_append_flag
{
    PLOG_FLAG_APPEND_DEFAULT              = 0x0000,
    PLOG_FLAG_APPEND_PARTIAL_EC_STRIPE    = 0x0001,  /* only partial ec stripe data that append into persistence layer, and will be padded as 0*/
    PLOG_FLAG_APPEND_FULL_EC_STRIPE       = 0x0002,
    PLOG_FLAG_APPEND_WRITE_BACK           = 0x0004,
    PLOG_FLAG_APPEND_WRITE_BACK_AND_PARTIAL_EC_STRIPE = 0x0001 | 0x0004,

    //TEST ONLY
    TEST_PLOG_FLAG_APPEND_MOCK_DATA       = 0x8000,  /* test only. For fast store data, the data would not be transfered through the NewWork, and will store as 0 on the storage device*/
}plog_append_flag_e;

typedef enum plog_read_flag
{
    PLOG_FLAG_READ_DEFAULT               = 0x0000,
    PLOG_FLAG_READ_REPLICA_WHEN_ERROR    = 0x0001,  /* read data from other replica when error occurs, this could miss some data that is processing on Primary node. */

    PLOG_FLAG_READ_WITHOUT_CHECK_CRC     = 0x0002,  /* do not verify crc on read */
    PLOG_FLAG_READ_WITHOUT_CACHE         = 0x0080,  /* do not put data in cache on read, just for SGl*/
}plog_read_flag_e;

/**
 * Create Options.
 */
typedef struct create_option
{
    plog_io_priority_e  priority;
    uint32_t            flag;
}create_option_t;

/**
 * Append Options.
 */
typedef struct append_option
{
    plog_io_priority_e  priority;
    uint32_t            flag;
}append_option_t;

/**
 * Read Options.
 */
typedef struct read_option
{
    plog_io_priority_e  priority;
    uint32_t            flag;
}read_option_t;

/**
 * IO data buffer.
 */
typedef struct plog_buffer
{
    char*    buf;                        /* data buffer */
    uint32_t len;                        /* buffer length */
}plog_buffer_t;

/**
 * IO buffer list.
 */
typedef struct plog_buffer_list
{
    uint16_t       cnt;                   /* cnt */
    plog_buffer_t* buffers;               /* bufs */
}plog_buffer_list_t;

/**
 *TraceInfo used for io trace.
 */
typedef void* trace_info_t;

/**
 * call back.
 */
typedef void(*plog_cb_func)(void *private_ctx, int ret_code);

typedef struct plog_call_back
{
    plog_cb_func   cb;                       /* callback */
    void *         private_ctx;              /* user context */
} plog_call_back_t;

/**
 * data to read.
 */
typedef struct plog_data_to_read
{
    uint64_t offset;                               /* offset */
    uint32_t length;                               /* len */
}plog_data_to_read_t;

/**
 * read vector.
 */
typedef struct plog_data_to_read_vec
{
    uint16_t              cnt;                      /* cnt */
    plog_data_to_read_t*  data_to_read;             /* data want to read */
}plog_data_to_read_vec_t;

/**
 * UUID struct
 */
typedef struct plog_userid_stru
{
    uint32_t userid_type;                       /* userid type��for exampe,used to indicate the type of database */
    uint32_t userid_len;                        /* valid data length in userid_buf,for database,use 32 bytes UUID*/
    uint8_t*  userid_buf;                        /* userid buffer,for database,use 32 bytes UUID*/
}plog_userid_t;

/***
 *     ####### Function definition Section #######
 */

/*******************************************************************************
* Function Description: Initialize PlogClient.

* Output Parameters:
            None

* Return Value:
            P_OK
            P_ERROR
* Comments:
*******************************************************************************/
int libclient_init(void);

/*******************************************************************************
* Function Description: PlogClient exit

* Input Parameters:
            None

* Output Parameters:
            None

* Return Value:
            P_OK
            P_ERROR
* Comments:
*******************************************************************************/
int libclient_exit(void);

/*******************************************************************************
* Function Description:
            query the system capacity by perftype&durability.
* Input Parameters:
            @type:       plog_perf_type_e
            @durability: the durability
            @trace_info:   the trace id which used to trace the api call
* Output Parameters:
            @total_capacity:  the total capacity of the perftype
            @used_capacity:   capacity already used
* Return Value:
            P_ARGS_INVAILD
            P_REQUEST_TIMEOUT
            P_BUSY
            P_OK
            P_ERROR
            P_PERF_AND_DUR_NOT_SUPPORTED
* Comments:
*******************************************************************************/
int query_capacity(const plog_perf_type_e type, const plog_durability_e durability, void* trace_info, uint64_t* total_capacity, uint64_t* used_capacity);

class PlogException : public std::exception {
   public:
    virtual const char* what() const noexcept {
        return _msg.c_str();
    }

    PlogException(const String& msg, plog_ret_code status)
        : _msg(msg), _status(status) {
    }

    plog_ret_code status() const {
        return _status;
    }

    virtual const std::string& str() const {
        return _msg;
    }

   private:
    std::string _msg;
    plog_ret_code _status;
};

/**
 *  16MB is the default limit set by K2 project, not general PLog which can be up to 2GB
 *  it can be changed by the method setPlogMaxSize(uint32_t plogMaxSize)
*/
constexpr uint32_t PLOG_MAX_SIZE = 2 * 1024 * 1024;

constexpr uint32_t DMA_ALIGNMENT = 4096;

constexpr uint32_t plogInfoSize = sizeof(PlogInfo);

} // ns k2
