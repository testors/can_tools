/**
 * @file stn2255.c
 * @brief STN2255 Protocol Extensions Implementation
 *
 * Reuses elm327_get_pid_info() and elm327_convert_pid_value() from elm327.h
 * for PID metadata and value conversion.
 */

#include "stn2255.h"
#include "elm327.h"
#include <stdio.h>
#include <string.h>
#include <ctype.h>

/*******************************************************************************
 * STN Detection
 ******************************************************************************/

bool stn2255_detect(const char *ati_resp, const char *sti_resp, stn2255_info_t *info)
{
    if (!info) return false;

    memset(info, 0, sizeof(*info));

    if (!sti_resp) return false;

    /* Check if STI response contains "STN" (case-insensitive) */
    const char *p = sti_resp;
    bool found = false;
    while (*p) {
        if ((p[0] == 'S' || p[0] == 's') &&
            (p[1] == 'T' || p[1] == 't') &&
            (p[2] == 'N' || p[2] == 'n')) {
            found = true;
            break;
        }
        p++;
    }

    if (!found) {
        /* STI returned '?' or non-STN response */
        return false;
    }

    info->is_stn = true;

    /* Copy STI response as device ID (trim whitespace) */
    const char *start = sti_resp;
    while (*start == ' ' || *start == '\r' || *start == '\n') start++;
    size_t len = strlen(start);
    while (len > 0 && (start[len - 1] == ' ' || start[len - 1] == '\r' || start[len - 1] == '\n')) {
        len--;
    }
    if (len >= sizeof(info->device_id)) len = sizeof(info->device_id) - 1;
    memcpy(info->device_id, start, len);
    info->device_id[len] = '\0';

    /* Copy ATI response as hardware ID if available */
    if (ati_resp) {
        start = ati_resp;
        while (*start == ' ' || *start == '\r' || *start == '\n') start++;
        len = strlen(start);
        while (len > 0 && (start[len - 1] == ' ' || start[len - 1] == '\r' || start[len - 1] == '\n')) {
            len--;
        }
        if (len >= sizeof(info->hardware_id)) len = sizeof(info->hardware_id) - 1;
        memcpy(info->hardware_id, start, len);
        info->hardware_id[len] = '\0';
    }

    return true;
}

/*******************************************************************************
 * STN Initialization
 ******************************************************************************/

void stn2255_build_init_cmds(char cmds[][STN2255_CMD_MAX_LEN], int *count)
{
    int n = 0;
    strncpy(cmds[n++], "STPTO 64",  STN2255_CMD_MAX_LEN);  /* Protocol timeout 100ms */
    strncpy(cmds[n++], "ATAL",      STN2255_CMD_MAX_LEN);   /* Allow long messages */
    strncpy(cmds[n++], "ATS1",      STN2255_CMD_MAX_LEN);   /* Spaces ON */
    strncpy(cmds[n++], "ATCAF1",    STN2255_CMD_MAX_LEN);   /* CAN Auto Formatting ON */
    *count = n;
}

/*******************************************************************************
 * Multi-PID Batch Query
 ******************************************************************************/

int stn2255_build_batch_query(const uint8_t *pids, int pid_count,
                               char *cmd, size_t cmd_len)
{
    if (!pids || !cmd || pid_count <= 0 || pid_count > STN2255_MAX_BATCH_PIDS) {
        return -1;
    }

    /* Build "01 0C 0D 11" format */
    int pos = 0;
    pos += snprintf(cmd + pos, cmd_len - pos, "01");

    for (int i = 0; i < pid_count; i++) {
        pos += snprintf(cmd + pos, cmd_len - pos, " %02X", pids[i]);
    }

    return 0;
}

/*******************************************************************************
 * Batch Response Parsing
 ******************************************************************************/

static int hex_val(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    return -1;
}

/**
 * @brief Try to parse one "41 XX DD..." PID response from a hex string
 *
 * @param hex Hex-only string (uppercase, no spaces)
 * @param hex_len Length of hex string
 * @param offset Start offset in hex string
 * @param result Output: parsed PID result
 * @return Number of hex chars consumed, or 0 on failure
 */
static int parse_one_pid_from_hex(const char *hex, int hex_len, int offset,
                                   stn2255_pid_result_t *result)
{
    /* Need at least "41XX" = 4 hex chars */
    if (offset + 4 > hex_len) return 0;

    /* Check mode 41 */
    if (hex[offset] != '4' || hex[offset + 1] != '1') return 0;

    /* Extract PID */
    int h = hex_val(hex[offset + 2]);
    int l = hex_val(hex[offset + 3]);
    if (h < 0 || l < 0) return 0;

    uint8_t pid = (uint8_t)((h << 4) | l);

    /* Get expected data byte count */
    const elm327_pid_info_t *info = elm327_get_pid_info(pid);
    int expected_bytes = info ? info->data_bytes : 0;

    /* If unknown PID, try to consume 1 byte as default */
    if (expected_bytes == 0) expected_bytes = 1;

    /* Check we have enough hex chars for data */
    int data_hex_len = expected_bytes * 2;
    if (offset + 4 + data_hex_len > hex_len) return 0;

    /* Extract data bytes */
    result->pid = pid;
    result->data_len = 0;
    result->valid = true;

    for (int i = 0; i < expected_bytes; i++) {
        int pos = offset + 4 + i * 2;
        h = hex_val(hex[pos]);
        l = hex_val(hex[pos + 1]);
        if (h < 0 || l < 0) {
            result->valid = false;
            return 0;
        }
        result->data[i] = (uint8_t)((h << 4) | l);
        result->data_len++;
    }

    /* Convert to engineering value */
    result->value = elm327_convert_pid_value(pid, result->data, result->data_len);

    return 4 + data_hex_len;
}

int stn2255_parse_batch_response(const char *response,
                                  stn2255_batch_result_t *result)
{
    if (!response || !result) return 0;

    memset(result, 0, sizeof(*result));

    /* Check for error responses */
    if (elm327_is_error_response(response)) return 0;

    /*
     * Strategy: Extract all hex chars, then parse "41XX..." sequences.
     * This handles all formats:
     *   - Multiline:  "410C1AF8\r\n410D55\r\n411137"
     *   - Packed:     "410C1AF8410D55411137"
     *   - With spaces: "41 0C 1A F8 41 0D 55 41 11 37"
     *
     * We also handle line-by-line in case each line has its own
     * non-hex prefix (e.g., CAN header bytes from multi-ECU responses).
     */

    /* First try line-by-line parsing */
    char line_buf[512];
    strncpy(line_buf, response, sizeof(line_buf) - 1);
    line_buf[sizeof(line_buf) - 1] = '\0';

    char *saveptr;
    char *line = strtok_r(line_buf, "\r\n", &saveptr);

    while (line && result->count < STN2255_MAX_BATCH_PIDS) {
        /* Skip empty lines */
        while (*line == ' ') line++;
        if (*line == '\0') {
            line = strtok_r(NULL, "\r\n", &saveptr);
            continue;
        }

        /* Extract hex from this line */
        char hex[128];
        int hex_len = elm327_extract_hex(line, hex, sizeof(hex));

        if (hex_len >= 4) {
            /* Try to parse multiple PIDs from this hex string */
            int offset = 0;
            while (offset < hex_len && result->count < STN2255_MAX_BATCH_PIDS) {
                /* Find next "41" marker */
                bool found_41 = false;
                while (offset + 1 < hex_len) {
                    if (hex[offset] == '4' && hex[offset + 1] == '1') {
                        found_41 = true;
                        break;
                    }
                    offset++;
                }
                if (!found_41) break;

                int consumed = parse_one_pid_from_hex(hex, hex_len, offset,
                                                       &result->results[result->count]);
                if (consumed > 0) {
                    result->count++;
                    offset += consumed;
                } else {
                    offset += 2; /* Skip past this "41" and try next */
                }
            }
        }

        line = strtok_r(NULL, "\r\n", &saveptr);
    }

    return result->count;
}

/*******************************************************************************
 * CAN Monitor
 ******************************************************************************/

void stn2255_build_monitor_cmd(char *cmd, size_t cmd_len)
{
    snprintf(cmd, cmd_len, "STMA");
}

void stn2255_build_filter_add(uint32_t can_id, char *cmd, size_t cmd_len)
{
    /* STFAP: Add pass filter (CAN ID, mask) */
    if (can_id > 0x7FF) {
        /* Extended 29-bit ID */
        snprintf(cmd, cmd_len, "STFAP %08X, %08X", can_id, 0x1FFFFFFF);
    } else {
        /* Standard 11-bit ID */
        snprintf(cmd, cmd_len, "STFAP %03X, 7FF", can_id);
    }
}

void stn2255_build_filter_clear(char *cmd, size_t cmd_len)
{
    snprintf(cmd, cmd_len, "STFAC");
}
