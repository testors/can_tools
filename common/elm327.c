/**
 * @file elm327.c
 * @brief ELM327 Protocol Handler Implementation
 *
 * Ported from:
 *   platforms/flutter/lib/src/services/obd/elm327_protocol.dart
 *   platforms/flutter/lib/src/services/obd/obd_pids.dart
 */

#include "elm327.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

/*******************************************************************************
 * PID Info Table
 ******************************************************************************/

static const elm327_pid_info_t s_pid_table[] = {
    { 0x04, "Engine Load",         "%",    1 },
    { 0x05, "Coolant Temp",        "°C",   1 },
    { 0x06, "STFT Bank 1",        "%",    1 },
    { 0x07, "LTFT Bank 1",        "%",    1 },
    { 0x0A, "Fuel Pressure",       "kPa",  1 },
    { 0x0B, "Intake Manifold",     "kPa",  1 },
    { 0x0C, "Engine RPM",          "rpm",  2 },
    { 0x0D, "Vehicle Speed",       "km/h", 1 },
    { 0x0E, "Timing Advance",      "°",    1 },
    { 0x0F, "Intake Air Temp",     "°C",   1 },
    { 0x10, "MAF Rate",            "g/s",  2 },
    { 0x11, "Throttle Position",   "%",    1 },
    { 0x14, "O2 B1S1",            "V",    2 },
    { 0x15, "O2 B1S2",            "V",    2 },
    { 0x1F, "Run Time",            "s",    2 },
    { 0x21, "Distance w/ MIL",    "km",   2 },
    { 0x22, "Fuel Rail Pressure",  "kPa",  2 },
    { 0x23, "Fuel Rail Gauge",     "kPa",  2 },
    { 0x2C, "Commanded EGR",       "%",    1 },
    { 0x2D, "EGR Error",           "%",    1 },
    { 0x2F, "Fuel Tank Level",     "%",    1 },
    { 0x33, "Barometric Pressure", "kPa",  1 },
    { 0x3C, "Catalyst Temp B1S1", "°C",   2 },
    { 0x42, "ECU Voltage",         "V",    2 },
    { 0x43, "Absolute Load",       "%",    2 },
    { 0x44, "Cmd Air/Fuel Ratio", "ratio", 2 },
    { 0x46, "Ambient Air Temp",    "°C",   1 },
    { 0x49, "Accel Pedal D",       "%",    1 },
    { 0x4A, "Accel Pedal E",       "%",    1 },
    { 0x4C, "Cmd Throttle",        "%",    1 },
    { 0x5C, "Engine Oil Temp",     "°C",   1 },
    { 0x5D, "Fuel Inj Timing",    "°",    2 },
    { 0x5E, "Fuel Rate",           "L/h",  2 },
    { 0x62, "Actual Torque",       "%",    1 },
    { 0x63, "Ref Torque",          "Nm",   2 },
};

static const int s_pid_table_count = sizeof(s_pid_table) / sizeof(s_pid_table[0]);

const elm327_pid_info_t* elm327_get_pid_info(uint8_t pid)
{
    for (int i = 0; i < s_pid_table_count; i++) {
        if (s_pid_table[i].pid == pid) {
            return &s_pid_table[i];
        }
    }
    return NULL;
}

/*******************************************************************************
 * AT Initialization
 ******************************************************************************/

void elm327_build_init_cmds(char cmds[][ELM327_CMD_MAX_LEN], int *count)
{
    int n = 0;
    strncpy(cmds[n++], "ATZ",   ELM327_CMD_MAX_LEN);  /* Reset */
    strncpy(cmds[n++], "ATE0",  ELM327_CMD_MAX_LEN);  /* Echo off */
    strncpy(cmds[n++], "ATL0",  ELM327_CMD_MAX_LEN);  /* Linefeeds off */
    strncpy(cmds[n++], "ATS0",  ELM327_CMD_MAX_LEN);  /* Spaces off */
    strncpy(cmds[n++], "ATSP0", ELM327_CMD_MAX_LEN);  /* Auto protocol */
    strncpy(cmds[n++], "ATI",   ELM327_CMD_MAX_LEN);  /* Identifier */
    strncpy(cmds[n++], "ATRV",  ELM327_CMD_MAX_LEN);  /* Voltage */
    strncpy(cmds[n++], "ATDPN", ELM327_CMD_MAX_LEN);  /* Protocol number */
    *count = n;
}

/*******************************************************************************
 * PID Query
 ******************************************************************************/

void elm327_build_pid_query(uint8_t pid, char *cmd, size_t len)
{
    snprintf(cmd, len, "01%02X", pid);
}

/*******************************************************************************
 * Hex Utilities
 ******************************************************************************/

static int hex_char_to_val(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    return -1;
}

int elm327_extract_hex(const char *resp, char *hex_out, size_t hex_out_len)
{
    size_t j = 0;
    for (size_t i = 0; resp[i] != '\0' && j < hex_out_len - 1; i++) {
        if (hex_char_to_val(resp[i]) >= 0) {
            hex_out[j++] = (char)toupper(resp[i]);
        }
    }
    hex_out[j] = '\0';
    return (int)j;
}

/*******************************************************************************
 * Response Parsing
 ******************************************************************************/

bool elm327_is_error_response(const char *resp)
{
    if (!resp) return true;

    /* Check common error strings */
    if (strstr(resp, "NO DATA"))            return true;
    if (strstr(resp, "ERROR"))              return true;
    if (strstr(resp, "UNABLE TO CONNECT"))  return true;
    if (strstr(resp, "BUS INIT"))           return true;
    if (strstr(resp, "CAN ERROR"))          return true;
    if (strstr(resp, "STOPPED"))            return true;

    /* Single '?' means unrecognized command */
    const char *p = resp;
    while (*p == ' ' || *p == '\r' || *p == '\n') p++;
    if (*p == '?' && (*(p + 1) == '\0' || *(p + 1) == '\r' || *(p + 1) == '\n')) {
        return true;
    }

    return false;
}

bool elm327_parse_pid_response(const char *resp, uint8_t *pid,
                                uint8_t *data, int *data_len)
{
    if (!resp || !pid || !data || !data_len) return false;

    /* Extract hex characters only */
    char hex[64];
    int hex_len = elm327_extract_hex(resp, hex, sizeof(hex));

    /* Need at least mode (41) + pid (XX) = 4 hex chars */
    if (hex_len < 4) return false;

    /* Check for mode 41 response */
    if (hex[0] != '4' || hex[1] != '1') return false;

    /* Extract PID */
    int h = hex_char_to_val(hex[2]);
    int l = hex_char_to_val(hex[3]);
    if (h < 0 || l < 0) return false;
    *pid = (uint8_t)((h << 4) | l);

    /* Get expected data bytes for this PID */
    const elm327_pid_info_t *info = elm327_get_pid_info(*pid);
    int expected = info ? info->data_bytes : 0;

    /* Extract data bytes from remaining hex */
    int n = 0;
    for (int i = 4; i + 1 < hex_len && n < ELM327_MAX_DATA_BYTES; i += 2) {
        h = hex_char_to_val(hex[i]);
        l = hex_char_to_val(hex[i + 1]);
        if (h < 0 || l < 0) break;
        data[n++] = (uint8_t)((h << 4) | l);
    }

    /* If we know expected count, validate */
    if (expected > 0 && n < expected) return false;

    *data_len = n;
    return true;
}

/*******************************************************************************
 * Value Conversion
 *
 * Each PID has its own conversion formula per SAE J1979 / ISO 15031-5.
 ******************************************************************************/

double elm327_convert_pid_value(uint8_t pid, const uint8_t *data, int len)
{
    if (!data || len <= 0) return 0.0;

    uint16_t ab = 0;
    if (len >= 2) {
        ab = (uint16_t)((data[0] << 8) | data[1]);
    }

    switch (pid) {
        /* Percent: A * 100 / 255 */
        case 0x04: /* Engine Load */
        case 0x11: /* Throttle Position */
        case 0x2C: /* Commanded EGR */
        case 0x2F: /* Fuel Tank Level */
        case 0x49: /* Accel Pedal D */
        case 0x4A: /* Accel Pedal E */
        case 0x4C: /* Cmd Throttle */
            return data[0] * 100.0 / 255.0;

        /* Temperature: A - 40 */
        case 0x05: /* Coolant Temp */
        case 0x0F: /* Intake Air Temp */
        case 0x46: /* Ambient Air Temp */
        case 0x5C: /* Engine Oil Temp */
            return data[0] - 40.0;

        /* Fuel trim: (A - 128) * 100 / 128 */
        case 0x06: /* STFT Bank 1 */
        case 0x07: /* LTFT Bank 1 */
            return (data[0] - 128) * 100.0 / 128.0;

        /* Fuel Pressure: A * 3 */
        case 0x0A:
            return data[0] * 3.0;

        /* Single byte direct (kPa, km/h) */
        case 0x0B: /* Intake Manifold Pressure */
        case 0x0D: /* Vehicle Speed */
        case 0x33: /* Barometric Pressure */
            return (double)data[0];

        /* RPM: ((A<<8)|B) / 4 */
        case 0x0C:
            if (len < 2) return 0.0;
            return ab / 4.0;

        /* Timing Advance: A/2 - 64 */
        case 0x0E:
            return data[0] / 2.0 - 64.0;

        /* MAF Rate: ((A<<8)|B) / 100 */
        case 0x10:
            if (len < 2) return 0.0;
            return ab / 100.0;

        /* Oxygen Sensor Voltage: A / 200 */
        case 0x14:
        case 0x15:
            return data[0] / 200.0;

        /* Two-byte unsigned: (A<<8)|B */
        case 0x1F: /* Run Time */
        case 0x21: /* Distance w/ MIL */
        case 0x63: /* Ref Torque (Nm) */
            if (len < 2) return 0.0;
            return (double)ab;

        /* Fuel Rail Pressure: ((A<<8)|B) * 0.079 */
        case 0x22:
            if (len < 2) return 0.0;
            return ab * 0.079;

        /* Fuel Rail Gauge Pressure: ((A<<8)|B) * 10 */
        case 0x23:
            if (len < 2) return 0.0;
            return ab * 10.0;

        /* EGR Error: (A - 128) * 100 / 128 */
        case 0x2D:
            return (data[0] - 128) * 100.0 / 128.0;

        /* Catalyst Temp: ((A<<8)|B) / 10 - 40 */
        case 0x3C:
            if (len < 2) return 0.0;
            return ab / 10.0 - 40.0;

        /* Control Module Voltage: ((A<<8)|B) / 1000 */
        case 0x42:
            if (len < 2) return 0.0;
            return ab / 1000.0;

        /* Absolute Load: ((A<<8)|B) * 100 / 255 */
        case 0x43:
            if (len < 2) return 0.0;
            return ab * 100.0 / 255.0;

        /* Air/Fuel Ratio: ((A<<8)|B) * 2 / 65536 */
        case 0x44:
            if (len < 2) return 0.0;
            return ab * 2.0 / 65536.0;

        /* Fuel Injection Timing: (((A<<8)|B) - 26880) / 128 */
        case 0x5D:
            if (len < 2) return 0.0;
            return ((int)ab - 26880) / 128.0;

        /* Fuel Rate: ((A<<8)|B) / 20 */
        case 0x5E:
            if (len < 2) return 0.0;
            return ab / 20.0;

        /* Actual Torque: A - 125 */
        case 0x62:
            return data[0] - 125.0;

        default:
            /* Unknown PID: return first byte as raw value */
            return (double)data[0];
    }
}

/*******************************************************************************
 * CAN Frame Parsing
 ******************************************************************************/

bool elm327_parse_can_frame(const char *line, can_frame_t *frame)
{
    if (!line || !frame) return false;

    /* Skip leading whitespace */
    while (*line == ' ' || *line == '\t') line++;

    /* Skip empty lines and error messages */
    if (*line == '\0' || *line == '\r' || *line == '\n') return false;

    /* Check for error keywords */
    if (elm327_is_error_response(line)) return false;

    /* Parse CAN ID (first hex token) */
    char *endptr;
    unsigned long can_id = strtoul(line, &endptr, 16);
    if (endptr == line) return false;

    /* Extended ID if > 0x7FF */
    bool is_extended = (can_id > 0x7FF);

    /* Parse data bytes */
    uint8_t data[8];
    int dlc = 0;
    const char *p = endptr;

    while (*p && dlc < 8) {
        /* Skip whitespace */
        while (*p == ' ' || *p == '\t') p++;
        if (*p == '\0' || *p == '\r' || *p == '\n') break;

        /* Parse hex byte */
        int h = hex_char_to_val(*p);
        if (h < 0) break;
        p++;

        int l = hex_char_to_val(*p);
        if (l < 0) break;
        p++;

        data[dlc++] = (uint8_t)((h << 4) | l);
    }

    if (dlc == 0) return false;

    memset(frame, 0, sizeof(*frame));
    frame->can_id = (uint32_t)can_id;
    frame->dlc = (uint8_t)dlc;
    frame->is_extended = is_extended;
    memcpy(frame->data, data, dlc);

    return true;
}

/*******************************************************************************
 * Supported PIDs
 ******************************************************************************/

void elm327_parse_supported_pids(const char *response_hex, uint8_t base_pid,
                                  bool supported[256])
{
    if (!response_hex || !supported) return;

    /* Extract hex-only from response */
    char hex[64];
    elm327_extract_hex(response_hex, hex, sizeof(hex));

    /* Skip mode (41) + PID (XX) = 4 hex chars to get 8-char bitmap */
    const char *bitmap_start = hex;
    size_t hex_len = strlen(hex);

    if (hex_len >= 12 && hex[0] == '4' && hex[1] == '1') {
        bitmap_start = hex + 4;
        hex_len -= 4;
    }

    if (hex_len < 8) return;

    /* Parse 4-byte (32-bit) bitmap */
    uint32_t bitmap = 0;
    for (int i = 0; i < 8; i++) {
        int v = hex_char_to_val(bitmap_start[i]);
        if (v < 0) return;
        bitmap = (bitmap << 4) | (uint32_t)v;
    }

    /* Each bit represents a PID: bit 31 = base_pid+1, bit 0 = base_pid+32 */
    for (int i = 0; i < 32; i++) {
        if (bitmap & (1u << (31 - i))) {
            int pid = base_pid + i + 1;
            if (pid < 256) {
                supported[pid] = true;
            }
        }
    }
}

/*******************************************************************************
 * Protocol Detection
 ******************************************************************************/

static const struct {
    char code;
    const char *name;
} s_protocols[] = {
    { '0', "Auto" },
    { '1', "SAE J1850 PWM" },
    { '2', "SAE J1850 VPW" },
    { '3', "ISO 9141-2" },
    { '4', "ISO 14230-4 KWP (5 baud init)" },
    { '5', "ISO 14230-4 KWP (fast init)" },
    { '6', "ISO 15765-4 CAN (11bit, 500kbaud)" },
    { '7', "ISO 15765-4 CAN (29bit, 500kbaud)" },
    { '8', "ISO 15765-4 CAN (11bit, 250kbaud)" },
    { '9', "ISO 15765-4 CAN (29bit, 250kbaud)" },
    { 'A', "SAE J1939 CAN" },
    { 'B', "User1 CAN" },
    { 'C', "User2 CAN" },
};

static const int s_protocol_count = sizeof(s_protocols) / sizeof(s_protocols[0]);

const char* elm327_parse_protocol(const char *response)
{
    if (!response) return NULL;

    /* Response format: "A6" where A=auto-detected prefix, digit=protocol */
    for (const char *p = response; *p; p++) {
        char c = (char)toupper(*p);
        /* Skip 'A' prefix (auto-detected marker) */
        if (c == 'A' && *(p + 1)) {
            c = (char)toupper(*(p + 1));
        }
        /* Find protocol by code */
        for (int i = 0; i < s_protocol_count; i++) {
            if (c == s_protocols[i].code) {
                return s_protocols[i].name;
            }
        }
    }

    return NULL;
}

/*******************************************************************************
 * Voltage Parsing
 ******************************************************************************/

bool elm327_parse_voltage(const char *resp, double *voltage)
{
    if (!resp || !voltage) return false;

    /* Find first digit or decimal point */
    const char *p = resp;
    while (*p && !(*p >= '0' && *p <= '9')) p++;
    if (!*p) return false;

    char *endptr;
    *voltage = strtod(p, &endptr);
    return (endptr != p);
}
