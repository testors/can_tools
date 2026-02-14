/**
 * @file stn2255.h
 * @brief STN2255 Protocol Extensions (Pure C)
 *
 * STN2255 chips (OBDLink MX+, some vLinker variants) are ELM327-compatible
 * but support additional ST commands for higher performance:
 *   - Multi-PID batch queries (1 round-trip instead of N)
 *   - Fine-grained protocol timeout (STPTO)
 *   - CAN monitoring with filter subsystem (STMA)
 *
 * Multi-PID batch queries ("01 0C 0D 11") are ISO 15765-4 standard and
 * work on ELM327 too, but STN provides better timing/filter optimization.
 */

#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*******************************************************************************
 * Constants
 ******************************************************************************/

/** Maximum PIDs in a single batch query */
#define STN2255_MAX_BATCH_PIDS  6

/** Maximum STN init commands */
#define STN2255_MAX_INIT_CMDS   4

/** Maximum command string length */
#define STN2255_CMD_MAX_LEN     64

/*******************************************************************************
 * STN Detection
 ******************************************************************************/

/**
 * @brief STN device information
 */
typedef struct {
    char device_id[64];     /**< STI response (e.g., "STN2255 v5.0.0") */
    char hardware_id[64];   /**< STDI response */
    bool is_stn;            /**< true if STN chip detected */
} stn2255_info_t;

/**
 * @brief Detect STN chip from ATI/STI responses
 *
 * Checks if the STI response contains "STN", indicating an STN-based adapter.
 *
 * @param ati_resp ATI response string (ELM327 identifier, may be NULL)
 * @param sti_resp STI response string (STN identifier)
 * @param info Output: detected STN info
 * @return true if STN chip detected
 */
bool stn2255_detect(const char *ati_resp, const char *sti_resp, stn2255_info_t *info);

/*******************************************************************************
 * STN Initialization
 ******************************************************************************/

/**
 * @brief Build STN optimization commands (called after ELM327 init)
 *
 * Generates:
 *   - STPTO 64  : Protocol timeout 100ms (finer than ATST)
 *   - ATAL      : Allow long messages (multi-PID ISO-TP responses)
 *   - ATS1      : Spaces ON (easier multi-PID response parsing)
 *   - ATCAF1    : CAN Auto Formatting ON (ISO-TP reassembly)
 *
 * @param cmds Output array of command strings
 * @param count Output number of commands
 */
void stn2255_build_init_cmds(char cmds[][STN2255_CMD_MAX_LEN], int *count);

/*******************************************************************************
 * Multi-PID Batch Query
 ******************************************************************************/

/**
 * @brief Single PID result within a batch response
 */
typedef struct {
    uint8_t pid;                /**< PID code */
    uint8_t data[8];            /**< Raw data bytes */
    int     data_len;           /**< Number of data bytes */
    double  value;              /**< Converted engineering value */
    bool    valid;              /**< true if parsed successfully */
} stn2255_pid_result_t;

/**
 * @brief Batch query result (up to STN2255_MAX_BATCH_PIDS)
 */
typedef struct {
    stn2255_pid_result_t results[STN2255_MAX_BATCH_PIDS];
    int count;                  /**< Number of results parsed */
} stn2255_batch_result_t;

/**
 * @brief Build multi-PID batch query command
 *
 * Generates "01 0C 0D 11" format for requesting multiple PIDs in one
 * CAN ISO-TP transaction. Works on both ELM327 and STN chips.
 *
 * @param pids Array of PID codes
 * @param pid_count Number of PIDs (max STN2255_MAX_BATCH_PIDS)
 * @param cmd Output command string buffer
 * @param cmd_len Buffer length
 * @return 0 on success, -1 on error
 */
int stn2255_build_batch_query(const uint8_t *pids, int pid_count,
                               char *cmd, size_t cmd_len);

/**
 * @brief Parse multi-PID batch response
 *
 * Handles multiple response formats:
 *   - Multiline:  "410C1AF8\n410D55\n411137"
 *   - Packed:     "410C1AF8410D55411137" (no spaces, continuous)
 *   - With spaces: "41 0C 1A F8 41 0D 55 41 11 37"
 *
 * Uses elm327_get_pid_info() for byte count and elm327_convert_pid_value()
 * for value conversion.
 *
 * @param response Response string from adapter
 * @param result Output: parsed results
 * @return Number of PIDs parsed (0 on error)
 */
int stn2255_parse_batch_response(const char *response,
                                  stn2255_batch_result_t *result);

/*******************************************************************************
 * CAN Monitor
 ******************************************************************************/

/**
 * @brief Build CAN monitor command (STMA for STN, ATMA for ELM327)
 * @param cmd Output command buffer
 * @param cmd_len Buffer length
 */
void stn2255_build_monitor_cmd(char *cmd, size_t cmd_len);

/**
 * @brief Build CAN filter add command (STFAP for STN)
 * @param can_id CAN ID to filter
 * @param cmd Output command buffer
 * @param cmd_len Buffer length
 */
void stn2255_build_filter_add(uint32_t can_id, char *cmd, size_t cmd_len);

/**
 * @brief Build CAN filter clear command (STFAC for STN)
 * @param cmd Output command buffer
 * @param cmd_len Buffer length
 */
void stn2255_build_filter_clear(char *cmd, size_t cmd_len);

#ifdef __cplusplus
}
#endif
