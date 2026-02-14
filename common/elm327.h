/**
 * @file elm327.h
 * @brief ELM327 Protocol Handler (Pure C)
 *
 * Handles ELM327 AT command generation, OBD-II PID query/response parsing,
 * value conversion, CAN frame parsing, and supported PID detection.
 *
 * Ported from platforms/flutter/lib/src/services/obd/elm327_protocol.dart
 * and obd_pids.dart.
 */

#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/**
 * @brief Raw CAN frame
 */
typedef struct {
    uint32_t can_id;            /**< CAN arbitration ID (11-bit or 29-bit) */
    uint8_t  data[8];           /**< Data bytes */
    uint8_t  dlc;               /**< Data length code (0-8) */
    int64_t  timestamp_ms;      /**< Timestamp in milliseconds */
    bool     is_extended;       /**< Extended (29-bit) ID flag */
} can_frame_t;

#ifdef __cplusplus
extern "C" {
#endif

/*******************************************************************************
 * Constants
 ******************************************************************************/

/** Maximum AT init commands */
#define ELM327_MAX_INIT_CMDS    8

/** Maximum command string length */
#define ELM327_CMD_MAX_LEN      32

/** Maximum response data bytes */
#define ELM327_MAX_DATA_BYTES   8

/** Maximum response line length */
#define ELM327_MAX_LINE_LEN     128

/*******************************************************************************
 * PID Info
 ******************************************************************************/

/**
 * @brief PID metadata (data byte count for parsing)
 */
typedef struct {
    uint8_t     pid;            /**< PID code */
    const char *name;           /**< Human-readable name */
    const char *unit;           /**< Unit of measurement */
    uint8_t     data_bytes;     /**< Expected data byte count */
} elm327_pid_info_t;

/**
 * @brief Get PID info
 * @param pid PID code
 * @return PID info or NULL if unknown
 */
const elm327_pid_info_t* elm327_get_pid_info(uint8_t pid);

/*******************************************************************************
 * AT Initialization
 ******************************************************************************/

/**
 * @brief Build AT initialization command sequence
 *
 * Generates: ATZ, ATE0, ATL0, ATS0, ATSP0, ATI, ATRV, ATDPN
 *
 * @param cmds Output array of command strings
 * @param count Output number of commands
 */
void elm327_build_init_cmds(char cmds[][ELM327_CMD_MAX_LEN], int *count);

/*******************************************************************************
 * PID Query
 ******************************************************************************/

/**
 * @brief Build Mode 01 PID query command
 *
 * Generates "01XX" format (e.g., "010C" for RPM).
 *
 * @param pid PID code
 * @param cmd Output command string buffer
 * @param len Buffer length
 */
void elm327_build_pid_query(uint8_t pid, char *cmd, size_t len);

/*******************************************************************************
 * Response Parsing
 ******************************************************************************/

/**
 * @brief Parse OBD-II PID response
 *
 * Parses "41XX..." response (with or without spaces) and extracts data bytes.
 *
 * @param resp Response string from ELM327
 * @param pid Output: parsed PID code
 * @param data Output: data bytes array
 * @param data_len Output: number of data bytes
 * @return true if parsing succeeded
 */
bool elm327_parse_pid_response(const char *resp, uint8_t *pid,
                                uint8_t *data, int *data_len);

/*******************************************************************************
 * Value Conversion
 ******************************************************************************/

/**
 * @brief Convert raw data bytes to engineering value
 *
 * Applies PID-specific conversion formula:
 *   RPM: ((A<<8)|B)/4
 *   Temperature: A-40
 *   Percent: A*100/255
 *   etc.
 *
 * @param pid PID code
 * @param data Raw data bytes
 * @param len Number of data bytes
 * @return Converted value in standard units, or 0.0 if unknown PID
 */
double elm327_convert_pid_value(uint8_t pid, const uint8_t *data, int len);

/*******************************************************************************
 * CAN Frame Parsing
 ******************************************************************************/

/**
 * @brief Parse CAN monitor response line
 *
 * Parses ATMA output format: "7E8 03 41 0C 1A F8"
 * (CAN ID followed by data bytes in hex)
 *
 * @param line Single line from CAN monitor output
 * @param frame Output: parsed CAN frame
 * @return true if parsing succeeded
 */
bool elm327_parse_can_frame(const char *line, can_frame_t *frame);

/*******************************************************************************
 * Supported PIDs
 ******************************************************************************/

/**
 * @brief Parse supported PIDs bitmap response
 *
 * Parses 4-byte bitmap from 0100/0120/0140/0160 responses.
 * Bitmap format: 32 bits, MSB first, each bit = one PID.
 *
 * @param response_hex Hex string of response (after stripping mode/pid prefix)
 * @param base_pid Base PID (0x00, 0x20, 0x40, or 0x60)
 * @param supported Output: boolean array indexed by PID [0..255]
 */
void elm327_parse_supported_pids(const char *response_hex, uint8_t base_pid,
                                  bool supported[256]);

/*******************************************************************************
 * Protocol Detection
 ******************************************************************************/

/**
 * @brief Parse ATDPN protocol number response
 *
 * @param response ATDPN response string
 * @return Protocol description string or NULL
 */
const char* elm327_parse_protocol(const char *response);

/*******************************************************************************
 * Response Utilities
 ******************************************************************************/

/**
 * @brief Extract hex-only characters from response
 *
 * Removes spaces, CR, LF, '>' prompt, and other non-hex chars.
 *
 * @param resp Input response string
 * @param hex_out Output hex string buffer
 * @param hex_out_len Buffer length
 * @return Length of extracted hex string
 */
int elm327_extract_hex(const char *resp, char *hex_out, size_t hex_out_len);

/**
 * @brief Check if response indicates an error
 *
 * Checks for "NO DATA", "ERROR", "UNABLE TO CONNECT", "?", etc.
 *
 * @param resp Response string
 * @return true if response is an error
 */
bool elm327_is_error_response(const char *resp);

/**
 * @brief Parse voltage from ATRV response
 *
 * Extracts float voltage value from response like "12.6V".
 *
 * @param resp ATRV response string
 * @param voltage Output voltage value
 * @return true if parsing succeeded
 */
bool elm327_parse_voltage(const char *resp, double *voltage);

#ifdef __cplusplus
}
#endif
