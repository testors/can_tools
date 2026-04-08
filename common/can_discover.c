/**
 * @file can_discover.c
 * @brief PT-CAN Signal Auto-Discovery — Analysis Logic (Pure C)
 */

#include "can_discover.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>

/*******************************************************************************
 * Signal Names
 ******************************************************************************/

static const char *s_signal_names[DISC_SIG_COUNT] = {
    "steering", "rpm", "throttle", "brake", "gear", "wheel_speed"
};

#define DISC_PRIOR_NAME_LEN   64
#define DISC_PRIOR_UNIT_LEN   32
#define DISC_PRIOR_VALUE_LEN  32

typedef struct {
    disc_signal_t signal;
    uint32_t can_id;
    uint8_t  dlc;
    int      start_bit;
    int      bit_length;
    int      byte_order;
    char     sign_char;
    char     name[DISC_PRIOR_NAME_LEN];
    char     factor[DISC_PRIOR_VALUE_LEN];
    char     offset[DISC_PRIOR_VALUE_LEN];
    char     min_val[DISC_PRIOR_VALUE_LEN];
    char     max_val[DISC_PRIOR_VALUE_LEN];
    char     unit[DISC_PRIOR_UNIT_LEN];
    int      support_count;
} disc_dbc_prior_t;

typedef struct {
    bool             matched;
    disc_dbc_prior_t prior;
} disc_prior_match_t;

typedef struct {
    int      start_bit;
    int      bit_length;
    int      byte_order;
    char     sign_char;
    int32_t  raw_min;
    int32_t  raw_max;
    int      active_unique;
    int      baseline_unique;
    double   score;
    uint64_t bitmask;
} disc_raw_guess_t;

typedef struct {
    char name[DISC_PRIOR_NAME_LEN];
    int  start_bit;
    int  bit_length;
    int  byte_order;
    char sign_char;
    char factor[DISC_PRIOR_VALUE_LEN];
    char offset[DISC_PRIOR_VALUE_LEN];
    char min_val[DISC_PRIOR_VALUE_LEN];
    char max_val[DISC_PRIOR_VALUE_LEN];
    char unit[DISC_PRIOR_UNIT_LEN];
    disc_signal_t signal;
    double quality;
    uint64_t bitmask;
} disc_export_signal_t;

typedef struct {
    char   *data;
    size_t  len;
    size_t  cap;
    bool    failed;
} disc_string_builder_t;

static int dbc_big_endian_next_bit(int bit);
static const char *signal_dbc_name(disc_signal_t sig);
static uint64_t field_bitmask(int start_bit, int bit_length, int byte_order);
static void dbc_signal_layout(const disc_candidate_t *cand,
                              int *start_bit, int *bit_length,
                              int *byte_order, char *sign_char);
static bool best_raw_guess(disc_signal_t signal,
                           const disc_raw_phase_t *baseline_raw,
                           const disc_raw_phase_t *active_raw,
                           const disc_candidate_t *cand,
                           disc_raw_guess_t *guess_out);
static double raw_guess_candidate_bonus(const disc_candidate_t *cand,
                                        const disc_raw_guess_t *guess);
static int best_multi_raw_guesses(disc_signal_t signal,
                                  const disc_raw_phase_t *baseline_raw,
                                  const disc_raw_phase_t *active_raw,
                                  const disc_candidate_t *cand,
                                  disc_raw_guess_t *guess_out,
                                  int max_guess_out);

#include "can_discover_priors.h"

const char *disc_signal_name(disc_signal_t sig) {
    if (sig < 0 || sig >= DISC_SIG_COUNT) return "unknown";
    return s_signal_names[sig];
}

static const char *s_conf_names[] = { "HIGH", "MEDIUM", "LOW" };
static const char *s_endian_names[] = { "unknown", "big", "little" };
static const char *s_sign_names[] = { "unknown", "unsigned", "signed" };

const char *disc_confidence_name(disc_confidence_t conf) {
    if (conf > DISC_CONF_LOW) return "UNKNOWN";
    return s_conf_names[conf];
}

const char *disc_endian_name(disc_endian_t e) {
    if (e > DISC_ENDIAN_LITTLE) return "unknown";
    return s_endian_names[e];
}

const char *disc_sign_name(disc_sign_t s) {
    if (s > DISC_SIGN_SIGNED) return "unknown";
    return s_sign_names[s];
}

/*******************************************************************************
 * Bus Mode Detection
 ******************************************************************************/

static const char *s_bus_mode_names[] = { "direct", "gateway" };

const char *disc_bus_mode_name(disc_bus_mode_t mode) {
    if (mode > DISC_BUS_GATEWAY) return "unknown";
    return s_bus_mode_names[mode];
}

/** qsort comparator for doubles */
static int cmp_double(const void *a, const void *b) {
    double da = *(const double *)a;
    double db = *(const double *)b;
    if (da < db) return -1;
    if (da > db) return 1;
    return 0;
}

/**
 * Detect bus mode from baseline capture statistics.
 * Computes Hz for each CAN ID, takes the median.
 * median < 10.0 → GATEWAY (D-CAN via ZGW), else DIRECT (PT-CAN).
 */
static disc_bus_mode_t detect_bus_mode(const disc_phase_t *baseline,
                                        double *out_hz_threshold) {
    if (baseline->id_count == 0 || baseline->duration_s <= 0) {
        *out_hz_threshold = 30.0;
        return DISC_BUS_DIRECT;
    }

    double hz_values[DISC_MAX_IDS];
    int n = baseline->id_count;
    for (int i = 0; i < n; i++) {
        hz_values[i] = (double)baseline->ids[i].frame_count / baseline->duration_s;
    }

    qsort(hz_values, n, sizeof(double), cmp_double);

    double median;
    if (n % 2 == 1) {
        median = hz_values[n / 2];
    } else {
        median = (hz_values[n / 2 - 1] + hz_values[n / 2]) / 2.0;
    }

    if (median < 10.0) {
        *out_hz_threshold = 1.0;
        return DISC_BUS_GATEWAY;
    } else {
        *out_hz_threshold = 30.0;
        return DISC_BUS_DIRECT;
    }
}

disc_bus_mode_t disc_detect_bus_mode(const disc_phase_t *baseline) {
    double threshold;
    return detect_bus_mode(baseline, &threshold);
}

/*******************************************************************************
 * Phase Capture
 ******************************************************************************/

void disc_result_init(disc_result_t *result) {
    if (!result) return;

    memset(result, 0, sizeof(*result));
    for (int i = 0; i < DISC_SIG_COUNT; i++) {
        result->signals[i].byte_idx = -1;
        result->signals[i].byte2_idx = -1;
    }
}

void disc_phase_init(disc_phase_t *phase) {
    memset(phase, 0, sizeof(*phase));
}

/**
 * Find or create an ID slot in the phase.
 * Returns index, or -1 if full.
 */
static int find_or_create_id(disc_phase_t *phase, uint32_t can_id, uint8_t dlc) {
    for (int i = 0; i < phase->id_count; i++) {
        if (phase->ids[i].can_id == can_id) {
            if (dlc > phase->ids[i].dlc) {
                /* Initialize newly exposed bytes */
                for (int b = phase->ids[i].dlc; b < dlc; b++) {
                    phase->ids[i].bytes[b].min_val = 0xFF;
                    phase->ids[i].bytes[b].max_val = 0x00;
                    phase->ids[i].bytes[b].unique_count = 0;
                    memset(phase->ids[i].bytes[b].seen, 0,
                           sizeof(phase->ids[i].bytes[b].seen));
                }
                phase->ids[i].dlc = dlc;
            }
            return i;
        }
    }
    if (phase->id_count >= DISC_MAX_IDS) return -1;

    int idx = phase->id_count++;
    disc_id_t *id = &phase->ids[idx];
    id->can_id = can_id;
    id->dlc = dlc;
    id->frame_count = 0;
    for (int b = 0; b < DISC_MAX_DLC; b++) {
        id->bytes[b].min_val = 0xFF;
        id->bytes[b].max_val = 0x00;
        id->bytes[b].unique_count = 0;
        memset(id->bytes[b].seen, 0, sizeof(id->bytes[b].seen));
    }
    return idx;
}

void disc_phase_add_frame(disc_phase_t *phase, uint32_t can_id,
                           const uint8_t *data, uint8_t dlc) {
    if (dlc > DISC_MAX_DLC) dlc = DISC_MAX_DLC;

    int idx = find_or_create_id(phase, can_id, dlc);
    if (idx < 0) return;

    disc_id_t *id = &phase->ids[idx];
    id->frame_count++;
    phase->total_frames++;

    for (int b = 0; b < dlc; b++) {
        disc_byte_t *bs = &id->bytes[b];
        uint8_t val = data[b];
        if (val < bs->min_val) bs->min_val = val;
        if (val > bs->max_val) bs->max_val = val;
        if (!bs->seen[val]) {
            bs->seen[val] = true;
            bs->unique_count++;
        }
    }
}

void disc_phase_finalize(disc_phase_t *phase, double duration_s) {
    phase->duration_s = duration_s;
}

void disc_raw_phase_init(disc_raw_phase_t *phase) {
    if (!phase) return;
    phase->frames = NULL;
    phase->frame_count = 0;
}

void disc_raw_phase_free(disc_raw_phase_t *phase) {
    if (!phase) return;
    free(phase->frames);
    phase->frames = NULL;
    phase->frame_count = 0;
}

bool disc_raw_phase_set(disc_raw_phase_t *phase,
                        const can_frame_t *frames,
                        int frame_count) {
    if (!phase || frame_count < 0) return false;

    can_frame_t *copy = NULL;
    if (frame_count > 0) {
        if (!frames) return false;
        copy = (can_frame_t *)malloc((size_t)frame_count * sizeof(*copy));
        if (!copy) return false;
        memcpy(copy, frames, (size_t)frame_count * sizeof(*copy));
    }

    free(phase->frames);
    phase->frames = copy;
    phase->frame_count = frame_count;
    return true;
}

/*******************************************************************************
 * Analysis Internals
 ******************************************************************************/

/** Per-byte comparison result */
typedef struct {
    int     byte_idx;
    double  score;              /* change score for this byte */
    bool    is_counter;         /* both phases have unique > 15 */
    bool    is_constant;        /* both phases have range == 0 */
    int     active_range;
    int     baseline_range;
    int     active_unique;
    int     baseline_unique;
} byte_cmp_t;

/** Per-CAN-ID comparison result */
typedef struct {
    uint32_t    can_id;
    uint8_t     dlc;
    double      hz;             /* frames / duration in active phase */
    double      total_score;    /* sum of non-ignored byte scores */
    int         changed_bytes;  /* count of bytes with score > 0 */
    byte_cmp_t  bytes[DISC_MAX_DLC];
    int         byte_count;
    int         best_byte;      /* byte index with highest score */
    int         second_byte;    /* byte index with second-highest score */
    int         pair_first;     /* first byte of strongest adjacent pair */
    int         pair_second;    /* second byte of strongest adjacent pair */
    double      pair_score;     /* combined score of strongest adjacent pair */
} id_cmp_t;

static double raw_byte_increment_ratio(const disc_raw_phase_t *raw_phase,
                                       uint32_t can_id,
                                       int byte_idx) {
    if (!raw_phase || !raw_phase->frames || raw_phase->frame_count < 4) return 0.0;
    if (byte_idx < 0 || byte_idx >= DISC_MAX_DLC) return 0.0;

    bool have_prev = false;
    uint8_t prev = 0;
    int changed = 0;
    int plus_one = 0;

    for (int i = 0; i < raw_phase->frame_count; i++) {
        const can_frame_t *frame = &raw_phase->frames[i];
        if (frame->can_id != can_id) continue;
        if (frame->dlc <= byte_idx) continue;

        uint8_t value = frame->data[byte_idx];
        if (have_prev) {
            if (value != prev) {
                changed++;
                if ((uint8_t)(value - prev) == 1) plus_one++;
            }
        }
        prev = value;
        have_prev = true;
    }

    if (changed < 4) return 0.0;
    return (double)plus_one / (double)changed;
}

/**
 * Compare one CAN ID between baseline and active phases.
 * Returns false if ID not found in both phases.
 */
static bool compare_id(const disc_phase_t *baseline, const disc_phase_t *active,
                       const disc_raw_phase_t *baseline_raw,
                       const disc_raw_phase_t *active_raw,
                       uint32_t can_id, id_cmp_t *out) {
    /* Find ID in both phases */
    const disc_id_t *base_id = NULL;
    const disc_id_t *act_id = NULL;

    for (int i = 0; i < baseline->id_count; i++) {
        if (baseline->ids[i].can_id == can_id) { base_id = &baseline->ids[i]; break; }
    }
    for (int i = 0; i < active->id_count; i++) {
        if (active->ids[i].can_id == can_id) { act_id = &active->ids[i]; break; }
    }

    if (!act_id) return false;

    memset(out, 0, sizeof(*out));
    out->can_id = can_id;
    out->dlc = act_id->dlc;
    out->hz = (active->duration_s > 0) ?
              (double)act_id->frame_count / active->duration_s : 0;

    int n = act_id->dlc;
    if (n > DISC_MAX_DLC) n = DISC_MAX_DLC;
    out->byte_count = n;

    double best_score = -1;
    double second_score = -1;
    out->best_byte = -1;
    out->second_byte = -1;
    out->pair_first = -1;
    out->pair_second = -1;
    out->pair_score = 0;

    for (int b = 0; b < n; b++) {
        byte_cmp_t *bc = &out->bytes[b];
        bc->byte_idx = b;

        const disc_byte_t *a = &act_id->bytes[b];
        int a_range = a->max_val - a->min_val;
        int a_unique = a->unique_count;

        int b_range = 0;
        int b_unique = 0;
        if (base_id) {
            const disc_byte_t *bl = &base_id->bytes[b];
            if (bl->unique_count > 0) {
                b_range = bl->max_val - bl->min_val;
                b_unique = bl->unique_count;
            }
        }

        bc->active_range = a_range;
        bc->baseline_range = b_range;
        bc->active_unique = a_unique;
        bc->baseline_unique = b_unique;

        /* Filter: counter/CRC — both have many unique values */
        if (a_unique > 15 && b_unique > 15) {
            bc->is_counter = true;
            bc->score = 0;
            continue;
        }

        double active_inc_ratio = raw_byte_increment_ratio(active_raw, can_id, b);
        double base_inc_ratio = raw_byte_increment_ratio(baseline_raw, can_id, b);
        if ((a_unique > 8 || b_unique > 8) &&
            (active_inc_ratio >= 0.78 || base_inc_ratio >= 0.78)) {
            bc->is_counter = true;
            bc->score = 0;
            continue;
        }

        /* Filter: constant — both have range 0 */
        if (a_range == 0 && b_range == 0) {
            bc->is_constant = true;
            bc->score = 0;
            continue;
        }

        /* Change score */
        double range_delta = (a_range > b_range) ? (a_range - b_range) : 0;
        double unique_delta = (a_unique > b_unique) ? (a_unique - b_unique) * 0.5 : 0;
        bc->score = range_delta + unique_delta;

        if (bc->score > 0) {
            out->changed_bytes++;
            out->total_score += bc->score;

            if (bc->score > best_score) {
                second_score = best_score;
                out->second_byte = out->best_byte;
                best_score = bc->score;
                out->best_byte = b;
            } else if (bc->score > second_score) {
                second_score = bc->score;
                out->second_byte = b;
            }
        }
    }

    for (int b = 0; b + 1 < n; b++) {
        double pair_score = out->bytes[b].score + out->bytes[b + 1].score;
        if (out->bytes[b].score <= 0 || out->bytes[b + 1].score <= 0) continue;
        if (pair_score > out->pair_score) {
            out->pair_score = pair_score;
            out->pair_first = b;
            out->pair_second = b + 1;
        }
    }

    return true;
}

/**
 * Compare all CAN IDs between baseline and active phases.
 * Returns number of comparable IDs.
 */
static int compare_phases(const disc_phase_t *baseline, const disc_phase_t *active,
                          const disc_raw_phase_t *baseline_raw,
                          const disc_raw_phase_t *active_raw,
                          id_cmp_t *results, int max_results) {
    int count = 0;

    /* Use active phase IDs as the reference set */
    for (int i = 0; i < active->id_count && count < max_results; i++) {
        id_cmp_t cmp;
        if (compare_id(baseline, active, baseline_raw, active_raw,
                       active->ids[i].can_id, &cmp)) {
            if (cmp.total_score > 0) {
                results[count++] = cmp;
            }
        }
    }

    /* Sort by total_score descending */
    for (int i = 0; i < count - 1; i++) {
        for (int j = i + 1; j < count; j++) {
            if (results[j].total_score > results[i].total_score) {
                id_cmp_t tmp = results[i];
                results[i] = results[j];
                results[j] = tmp;
            }
        }
    }

    return count;
}

/** Check if a CAN ID is in the exclusion list */
static bool is_excluded(uint32_t can_id, const uint32_t *excl, int excl_count) {
    for (int i = 0; i < excl_count; i++) {
        if (excl[i] == can_id) return true;
    }
    return false;
}

/** Fill a candidate from an id_cmp result */
static void fill_candidate(disc_candidate_t *cand, const id_cmp_t *cmp) {
    cand->can_id = cmp->can_id;
    cand->dlc = cmp->dlc;
    cand->hz = cmp->hz;
    cand->byte_idx = cmp->best_byte;
    cand->byte2_idx = -1;
    cand->score = cmp->total_score;
}

static bool cmp_has_adjacent_pair(const id_cmp_t *cmp, double min_secondary_score) {
    if (!cmp || cmp->pair_first < 0 || cmp->pair_second < 0) return false;
    if (cmp->bytes[cmp->pair_first].score < min_secondary_score) return false;
    if (cmp->bytes[cmp->pair_second].score < min_secondary_score) return false;
    return true;
}

static void candidate_use_adjacent_pair(disc_candidate_t *cand, const id_cmp_t *cmp) {
    if (!cand || !cmp) return;
    if (cmp->pair_first < 0 || cmp->pair_second < 0) return;
    cand->byte_idx = cmp->pair_first;
    cand->byte2_idx = cmp->pair_second;
}

/**
 * Find the best non-excluded CAN ID with optional preference criteria.
 * Returns index in results[] or -1.
 */
static int find_best(const id_cmp_t *results, int count,
                       const uint32_t *excl, int excl_count) {
    for (int i = 0; i < count; i++) {
        if (!is_excluded(results[i].can_id, excl, excl_count)) {
            return i;
        }
    }
    return -1;
}

/*******************************************************************************
 * Signal Classification
 ******************************************************************************/

/**
 * Classify steering: highest score in steering phase.
 * Prefer IDs with 2 adjacent changing bytes (16-bit angle).
 */
static bool classify_steering(const id_cmp_t *results, int count,
                                const uint32_t *excl, int excl_count,
                                disc_candidate_t *out) {
    /* First pass: look for 2 adjacent changed bytes (16-bit signal) */
    for (int i = 0; i < count; i++) {
        if (is_excluded(results[i].can_id, excl, excl_count)) continue;

        const id_cmp_t *r = &results[i];
        if (cmp_has_adjacent_pair(r, 1.0)) {
            fill_candidate(out, r);
            candidate_use_adjacent_pair(out, r);
            out->confidence = DISC_CONF_HIGH;
            return true;
        }
    }

    /* Second pass: just take highest score */
    int idx = find_best(results, count, excl, excl_count);
    if (idx >= 0) {
        fill_candidate(out, &results[idx]);
        out->confidence = (results[idx].total_score >= 20.0)
                          ? DISC_CONF_MEDIUM : DISC_CONF_LOW;
        return true;
    }
    return false;
}

/**
 * Classify throttle + RPM from throttle phase:
 *   - Throttle: fewest changed_bytes or smallest DLC (pedal = simple)
 *   - RPM: highest Hz + multi-byte change (engine = high-frequency)
 */
static void classify_throttle_rpm(const id_cmp_t *results, int count,
                                    const uint32_t *excl, int excl_count,
                                    disc_candidate_t *throttle_out, bool *throttle_found,
                                    disc_candidate_t *rpm_out, bool *rpm_found,
                                    double hz_threshold) {
    *throttle_found = false;
    *rpm_found = false;

    /* Collect top non-excluded candidates */
    int top_idx[8];
    int top_count = 0;
    for (int i = 0; i < count && top_count < 8; i++) {
        if (!is_excluded(results[i].can_id, excl, excl_count)) {
            top_idx[top_count++] = i;
        }
    }

    if (top_count == 0) return;

    if (top_count == 1) {
        /* Only one candidate — assign to RPM (more valuable for motorsport) */
        fill_candidate(rpm_out, &results[top_idx[0]]);
        if (cmp_has_adjacent_pair(&results[top_idx[0]], 1.0))
            candidate_use_adjacent_pair(rpm_out, &results[top_idx[0]]);
        rpm_out->confidence = DISC_CONF_MEDIUM;
        *rpm_found = true;
        return;
    }

    /* Strategy: separate RPM (high Hz, multi-byte) from throttle (simple) */
    int rpm_idx = -1;
    int throttle_idx = -1;

    /* RPM: prefer highest Hz among candidates with multi-byte change */
    double best_hz = -1;
    for (int i = 0; i < top_count; i++) {
        const id_cmp_t *r = &results[top_idx[i]];
        if (r->changed_bytes >= 2 && r->hz > best_hz) {
            best_hz = r->hz;
            rpm_idx = i;
        }
    }

    /* If no multi-byte, just pick highest Hz */
    if (rpm_idx < 0) {
        best_hz = -1;
        for (int i = 0; i < top_count; i++) {
            if (results[top_idx[i]].hz > best_hz) {
                best_hz = results[top_idx[i]].hz;
                rpm_idx = i;
            }
        }
    }

    /* Throttle: among remaining, pick smallest DLC or fewest changed_bytes */
    int best_simplicity = 9999;
    for (int i = 0; i < top_count; i++) {
        if (i == rpm_idx) continue;
        const id_cmp_t *r = &results[top_idx[i]];
        int simplicity = r->changed_bytes * 10 + r->dlc;
        if (simplicity < best_simplicity) {
            best_simplicity = simplicity;
            throttle_idx = i;
        }
    }

    if (rpm_idx >= 0) {
        fill_candidate(rpm_out, &results[top_idx[rpm_idx]]);
        const id_cmp_t *rr = &results[top_idx[rpm_idx]];
        if (cmp_has_adjacent_pair(rr, 1.0))
            candidate_use_adjacent_pair(rpm_out, rr);
        rpm_out->confidence = (rr->changed_bytes >= 2 && rr->hz >= hz_threshold)
                              ? DISC_CONF_HIGH : DISC_CONF_MEDIUM;
        *rpm_found = true;
    }
    if (throttle_idx >= 0) {
        fill_candidate(throttle_out, &results[top_idx[throttle_idx]]);
        throttle_out->confidence = (top_count >= 2) ? DISC_CONF_HIGH : DISC_CONF_MEDIUM;
        *throttle_found = true;
    }
}

/**
 * Classify brake: highest score in brake phase (excluding already-found IDs).
 */
static bool classify_brake(const id_cmp_t *results, int count,
                             const uint32_t *excl, int excl_count,
                             disc_candidate_t *out) {
    int idx = find_best(results, count, excl, excl_count);
    if (idx >= 0) {
        fill_candidate(out, &results[idx]);
        if (cmp_has_adjacent_pair(&results[idx], 2.0))
            candidate_use_adjacent_pair(out, &results[idx]);
        out->confidence = (results[idx].total_score >= 20.0)
                          ? DISC_CONF_HIGH
                          : (results[idx].total_score >= 8.0)
                            ? DISC_CONF_MEDIUM : DISC_CONF_LOW;
        return true;
    }
    return false;
}

/**
 * Classify gear: discrete enum pattern in gear phase.
 *   - Active unique values 3-8 (P/R/N/D/S/M...)
 *   - Baseline unique ≤ 2
 * Falls back to highest score if no enum pattern found.
 */
static bool classify_gear(const id_cmp_t *results, int count,
                            const uint32_t *excl, int excl_count,
                            disc_candidate_t *out) {
    /* First pass: look for discrete enum pattern */
    for (int i = 0; i < count; i++) {
        if (is_excluded(results[i].can_id, excl, excl_count)) continue;

        const id_cmp_t *r = &results[i];
        /* Check each byte for enum-like pattern */
        for (int b = 0; b < r->byte_count; b++) {
            const byte_cmp_t *bc = &r->bytes[b];
            if (bc->is_counter || bc->is_constant) continue;
            if (bc->active_unique >= 3 && bc->active_unique <= 8 &&
                bc->baseline_unique <= 2 && bc->score > 0) {
                fill_candidate(out, r);
                out->byte_idx = b;
                out->byte2_idx = -1;
                out->confidence = DISC_CONF_HIGH;
                return true;
            }
        }
    }

    /* Fallback: highest score */
    int idx = find_best(results, count, excl, excl_count);
    if (idx >= 0) {
        fill_candidate(out, &results[idx]);
        out->confidence = DISC_CONF_LOW;
        return true;
    }
    return false;
}

/**
 * Classify wheel speed: requires driving phase (stationary baseline = constant).
 *   - 4+ changing bytes at high Hz → 4-wheel speed in one message
 *   - 2+ changing bytes at high Hz → 2-wheel per message
 *   - Falls back to highest score
 */
static bool classify_wheel_speed(const id_cmp_t *results, int count,
                                   const uint32_t *excl, int excl_count,
                                   disc_candidate_t *out,
                                   double hz_threshold) {
    /* 1st pass: 4+ bytes changing + high Hz (4-wheel pattern) */
    for (int i = 0; i < count; i++) {
        if (is_excluded(results[i].can_id, excl, excl_count)) continue;
        const id_cmp_t *r = &results[i];
        if (r->changed_bytes >= 4 && r->hz >= hz_threshold) {
            fill_candidate(out, r);
            if (cmp_has_adjacent_pair(r, 1.0))
                candidate_use_adjacent_pair(out, r);
            out->confidence = DISC_CONF_HIGH;
            return true;
        }
    }

    /* 2nd pass: 2+ bytes changing + high Hz (2-wheel per message) */
    for (int i = 0; i < count; i++) {
        if (is_excluded(results[i].can_id, excl, excl_count)) continue;
        const id_cmp_t *r = &results[i];
        if (r->changed_bytes >= 2 && r->hz >= hz_threshold) {
            fill_candidate(out, r);
            if (cmp_has_adjacent_pair(r, 1.0))
                candidate_use_adjacent_pair(out, r);
            out->confidence = DISC_CONF_MEDIUM;
            return true;
        }
    }

    /* Fallback: highest score */
    int idx = find_best(results, count, excl, excl_count);
    if (idx >= 0) {
        fill_candidate(out, &results[idx]);
        if (cmp_has_adjacent_pair(&results[idx], 1.0))
            candidate_use_adjacent_pair(out, &results[idx]);
        out->confidence = DISC_CONF_LOW;
        return true;
    }
    return false;
}

/*******************************************************************************
 * Signal Characterization
 ******************************************************************************/

/** Find a CAN ID in a phase. Returns NULL if not found. */
static const disc_id_t *find_id_in_phase(const disc_phase_t *phase,
                                           uint32_t can_id) {
    for (int i = 0; i < phase->id_count; i++) {
        if (phase->ids[i].can_id == can_id) return &phase->ids[i];
    }
    return NULL;
}

/**
 * Check if MSB byte has a bimodal distribution around the 0x00/0xFF boundary,
 * indicating a signed value that crosses zero.
 */
static bool msb_looks_signed(const disc_byte_t *msb) {
    bool near_zero = false;     /* values in [0x00, 0x1F] */
    bool near_ff = false;       /* values in [0xE0, 0xFF] */
    bool has_middle = false;    /* values in [0x40, 0xBF] */

    for (int v = 0; v <= 0x1F; v++)
        if (msb->seen[v]) { near_zero = true; break; }
    for (int v = 0xE0; v <= 0xFF; v++)
        if (msb->seen[v]) { near_ff = true; break; }
    for (int v = 0x40; v <= 0xBF; v++)
        if (msb->seen[v]) { has_middle = true; break; }

    return near_zero && near_ff && !has_middle;
}

void disc_characterize(const disc_phase_t *active, disc_candidate_t *cand) {
    cand->endianness = DISC_ENDIAN_UNKNOWN;
    cand->signedness = DISC_SIGN_UNKNOWN;
    cand->raw_min = 0;
    cand->raw_max = 0;

    const disc_id_t *id = find_id_in_phase(active, cand->can_id);
    if (!id || cand->byte_idx < 0) return;

    if (cand->byte2_idx >= 0 && abs(cand->byte_idx - cand->byte2_idx) != 1) {
        cand->byte2_idx = -1;
    }

    if (cand->byte2_idx < 0) {
        /* --- Single-byte signal --- */
        const disc_byte_t *b = &id->bytes[cand->byte_idx];
        cand->signedness = DISC_SIGN_UNSIGNED;
        cand->raw_min = b->min_val;
        cand->raw_max = b->max_val;
        return;
    }

    /* --- Two-byte signal --- */
    const disc_byte_t *b1 = &id->bytes[cand->byte_idx];
    const disc_byte_t *b2 = &id->bytes[cand->byte2_idx];

    int range1 = b1->max_val - b1->min_val;
    int range2 = b2->max_val - b2->min_val;

    /* MSB has smaller range (fewer distinct high-order values) */
    int msb_idx, lsb_idx;
    const disc_byte_t *msb, *lsb;
    if (range1 <= range2) {
        msb_idx = cand->byte_idx;  lsb_idx = cand->byte2_idx;
        msb = b1; lsb = b2;
    } else {
        msb_idx = cand->byte2_idx; lsb_idx = cand->byte_idx;
        msb = b2; lsb = b1;
    }

    /* Endianness: MSB at lower address = big-endian */
    if (msb_idx < lsb_idx)
        cand->endianness = DISC_ENDIAN_BIG;
    else
        cand->endianness = DISC_ENDIAN_LITTLE;

    /* Reorder byte_idx/byte2_idx: byte_idx = MSB, byte2_idx = LSB */
    cand->byte_idx = msb_idx;
    cand->byte2_idx = lsb_idx;

    /* Signedness: check MSB for bimodal zero-crossing pattern */
    bool is_signed = msb_looks_signed(msb);
    cand->signedness = is_signed ? DISC_SIGN_SIGNED : DISC_SIGN_UNSIGNED;

    /* Raw range (approximate from per-byte stats) */
    uint16_t u_min = ((uint16_t)msb->min_val << 8) | lsb->min_val;
    uint16_t u_max = ((uint16_t)msb->max_val << 8) | lsb->max_val;

    if (is_signed) {
        cand->raw_min = (int16_t)u_min;
        cand->raw_max = (int16_t)u_max;
        /* Ensure min <= max for signed (wrap-around case) */
        if (cand->raw_min > cand->raw_max) {
            int32_t tmp = cand->raw_min;
            cand->raw_min = cand->raw_max;
            cand->raw_max = tmp;
        }
    } else {
        cand->raw_min = u_min;
        cand->raw_max = u_max;
    }
}

/*******************************************************************************
 * Per-Signal Classification (Public API)
 ******************************************************************************/

bool disc_classify_with_raw(const disc_phase_t *baseline,
                            const disc_raw_phase_t *baseline_raw,
                            const disc_phase_t *active,
                            const disc_raw_phase_t *active_raw,
                            disc_signal_t signal,
                            const uint32_t *excl, int excl_count,
                            disc_candidate_t *out,
                            disc_candidate_t *rpm_out, bool *rpm_found) {
    /* Detect bus mode to set adaptive Hz threshold */
    double hz_threshold;
    detect_bus_mode(baseline, &hz_threshold);

    id_cmp_t cmp_buf[DISC_MAX_IDS];
    int n = compare_phases(baseline, active, baseline_raw, active_raw,
                           cmp_buf, DISC_MAX_IDS);

    memset(out, 0, sizeof(*out));
    out->byte_idx = -1;
    out->byte2_idx = -1;

    switch (signal) {
    case DISC_SIG_STEERING:
        return classify_steering(cmp_buf, n, excl, excl_count, out);
    case DISC_SIG_THROTTLE: {
        /* Throttle phase detects both RPM and throttle */
        bool t_found = false, r_found = false;
        disc_candidate_t t_cand = {0}, r_cand = {0};
        t_cand.byte_idx = -1; t_cand.byte2_idx = -1;
        r_cand.byte_idx = -1; r_cand.byte2_idx = -1;
        classify_throttle_rpm(cmp_buf, n, excl, excl_count,
                               &t_cand, &t_found, &r_cand, &r_found,
                               hz_threshold);
        if (t_found) *out = t_cand;
        if (rpm_out && rpm_found) {
            *rpm_found = r_found;
            if (r_found) *rpm_out = r_cand;
        }
        return t_found;
    }
    case DISC_SIG_RPM:
        /* RPM is detected via DISC_SIG_THROTTLE; standalone not supported */
        return false;
    case DISC_SIG_BRAKE:
        return classify_brake(cmp_buf, n, excl, excl_count, out);
    case DISC_SIG_GEAR:
        return classify_gear(cmp_buf, n, excl, excl_count, out);
    case DISC_SIG_WHEEL_SPEED:
        return classify_wheel_speed(cmp_buf, n, excl, excl_count, out,
                                     hz_threshold);
    default:
        return false;
    }
}

bool disc_classify(const disc_phase_t *baseline,
                   const disc_phase_t *active,
                   disc_signal_t signal,
                   const uint32_t *excl, int excl_count,
                   disc_candidate_t *out,
                   disc_candidate_t *rpm_out, bool *rpm_found) {
    return disc_classify_with_raw(baseline, NULL, active, NULL,
                                  signal, excl, excl_count,
                                  out, rpm_out, rpm_found);
}

/*******************************************************************************
 * Main Analysis (Batch)
 ******************************************************************************/

static void disc_analyze_internal(const disc_phase_t phases[DISC_NUM_PHASES],
                                  const disc_raw_phase_t *raw_phases,
                                  disc_result_t *result) {
    if (!phases || !result) return;

    disc_result_init(result);
    const disc_phase_t *baseline = &phases[DISC_PHASE_BASELINE];

    /* Detect bus mode and adaptive Hz threshold */
    double hz_threshold;
    result->bus_mode = detect_bus_mode(baseline, &hz_threshold);

    /* Keep exclusions empty so multiple signals can share one CAN message. */
    uint32_t excl[DISC_SIG_COUNT] = {0};
    int excl_count = 0;
    disc_candidate_t rpm_cand;
    bool rpm_found = false;

    if (disc_classify_with_raw(baseline,
                               raw_phases ? &raw_phases[DISC_PHASE_BASELINE] : NULL,
                               &phases[DISC_PHASE_STEERING],
                               raw_phases ? &raw_phases[DISC_PHASE_STEERING] : NULL,
                               DISC_SIG_STEERING,
                               excl, excl_count,
                               &result->signals[DISC_SIG_STEERING],
                               NULL, NULL)) {
        result->found[DISC_SIG_STEERING] = true;
        disc_characterize(&phases[DISC_PHASE_STEERING],
                          &result->signals[DISC_SIG_STEERING]);
    }

    rpm_cand.byte_idx = -1;
    rpm_cand.byte2_idx = -1;
    if (disc_classify_with_raw(baseline,
                               raw_phases ? &raw_phases[DISC_PHASE_BASELINE] : NULL,
                               &phases[DISC_PHASE_THROTTLE],
                               raw_phases ? &raw_phases[DISC_PHASE_THROTTLE] : NULL,
                               DISC_SIG_THROTTLE,
                               excl, excl_count,
                               &result->signals[DISC_SIG_THROTTLE],
                               &rpm_cand, &rpm_found)) {
        result->found[DISC_SIG_THROTTLE] = true;
        disc_characterize(&phases[DISC_PHASE_THROTTLE],
                          &result->signals[DISC_SIG_THROTTLE]);
    }
    if (rpm_found) {
        result->signals[DISC_SIG_RPM] = rpm_cand;
        result->found[DISC_SIG_RPM] = true;
        disc_characterize(&phases[DISC_PHASE_THROTTLE],
                          &result->signals[DISC_SIG_RPM]);
    }

    if (disc_classify_with_raw(baseline,
                               raw_phases ? &raw_phases[DISC_PHASE_BASELINE] : NULL,
                               &phases[DISC_PHASE_BRAKE],
                               raw_phases ? &raw_phases[DISC_PHASE_BRAKE] : NULL,
                               DISC_SIG_BRAKE,
                               excl, excl_count,
                               &result->signals[DISC_SIG_BRAKE],
                               NULL, NULL)) {
        result->found[DISC_SIG_BRAKE] = true;
        disc_characterize(&phases[DISC_PHASE_BRAKE],
                          &result->signals[DISC_SIG_BRAKE]);
    }

    if (disc_classify_with_raw(baseline,
                               raw_phases ? &raw_phases[DISC_PHASE_BASELINE] : NULL,
                               &phases[DISC_PHASE_GEAR],
                               raw_phases ? &raw_phases[DISC_PHASE_GEAR] : NULL,
                               DISC_SIG_GEAR,
                               excl, excl_count,
                               &result->signals[DISC_SIG_GEAR],
                               NULL, NULL)) {
        result->found[DISC_SIG_GEAR] = true;
        disc_characterize(&phases[DISC_PHASE_GEAR],
                          &result->signals[DISC_SIG_GEAR]);
    }

    if (disc_classify_with_raw(baseline,
                               raw_phases ? &raw_phases[DISC_PHASE_BASELINE] : NULL,
                               &phases[DISC_PHASE_WHEEL_SPEED],
                               raw_phases ? &raw_phases[DISC_PHASE_WHEEL_SPEED] : NULL,
                               DISC_SIG_WHEEL_SPEED,
                               excl, excl_count,
                               &result->signals[DISC_SIG_WHEEL_SPEED],
                               NULL, NULL)) {
        result->found[DISC_SIG_WHEEL_SPEED] = true;
        disc_characterize(&phases[DISC_PHASE_WHEEL_SPEED],
                          &result->signals[DISC_SIG_WHEEL_SPEED]);
    }
}

void disc_analyze(const disc_phase_t phases[DISC_NUM_PHASES],
                  disc_result_t *result) {
    disc_analyze_internal(phases, NULL, result);
}

void disc_analyze_with_raw(const disc_phase_t phases[DISC_NUM_PHASES],
                           const disc_raw_phase_t raw_phases[DISC_NUM_PHASES],
                           disc_result_t *result) {
    disc_analyze_internal(phases, raw_phases, result);
}

/*******************************************************************************
 * Draft DBC Export
 ******************************************************************************/

static bool disc_sb_reserve(disc_string_builder_t *sb, size_t extra) {
    if (!sb || sb->failed) return false;
    if (extra > SIZE_MAX - sb->len - 1) {
        sb->failed = true;
        return false;
    }

    size_t need = sb->len + extra + 1;
    if (need <= sb->cap) return true;

    size_t new_cap = sb->cap ? sb->cap : 1024;
    while (new_cap < need) {
        if (new_cap > SIZE_MAX / 2) {
            new_cap = need;
            break;
        }
        new_cap *= 2;
    }

    char *new_data = (char *)realloc(sb->data, new_cap);
    if (!new_data) {
        sb->failed = true;
        return false;
    }

    sb->data = new_data;
    sb->cap = new_cap;
    return true;
}

static bool disc_sb_appendf(disc_string_builder_t *sb, const char *fmt, ...) {
    if (!sb || !fmt) return false;

    va_list ap;
    va_start(ap, fmt);
    va_list ap_copy;
    va_copy(ap_copy, ap);
    int needed = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);
    if (needed < 0) {
        va_end(ap_copy);
        sb->failed = true;
        return false;
    }

    if (!disc_sb_reserve(sb, (size_t)needed)) {
        va_end(ap_copy);
        return false;
    }

    vsnprintf(sb->data + sb->len, sb->cap - sb->len, fmt, ap_copy);
    va_end(ap_copy);
    sb->len += (size_t)needed;
    return true;
}

static int signal_phase_index(disc_signal_t sig) {
    switch (sig) {
    case DISC_SIG_STEERING:    return DISC_PHASE_STEERING;
    case DISC_SIG_RPM:         return DISC_PHASE_THROTTLE;
    case DISC_SIG_THROTTLE:    return DISC_PHASE_THROTTLE;
    case DISC_SIG_BRAKE:       return DISC_PHASE_BRAKE;
    case DISC_SIG_GEAR:        return DISC_PHASE_GEAR;
    case DISC_SIG_WHEEL_SPEED: return DISC_PHASE_WHEEL_SPEED;
    default:                   return DISC_PHASE_BASELINE;
    }
}

static const char *signal_dbc_name(disc_signal_t sig) {
    switch (sig) {
    case DISC_SIG_STEERING:    return "Steering";
    case DISC_SIG_RPM:         return "RPM";
    case DISC_SIG_THROTTLE:    return "Throttle";
    case DISC_SIG_BRAKE:       return "Brake";
    case DISC_SIG_GEAR:        return "Gear";
    case DISC_SIG_WHEEL_SPEED: return "WheelSpeed";
    default:                   return "Signal";
    }
}

static void fill_export_from_prior(disc_export_signal_t *out,
                                   const disc_dbc_prior_t *prior) {
    snprintf(out->name, sizeof(out->name), "%s", prior->name);
    out->start_bit = prior->start_bit;
    out->bit_length = prior->bit_length;
    out->byte_order = prior->byte_order;
    out->sign_char = prior->sign_char;
    snprintf(out->factor, sizeof(out->factor), "%s", prior->factor);
    snprintf(out->offset, sizeof(out->offset), "%s", prior->offset);
    snprintf(out->min_val, sizeof(out->min_val), "%s", prior->min_val);
    snprintf(out->max_val, sizeof(out->max_val), "%s", prior->max_val);
    snprintf(out->unit, sizeof(out->unit), "%s", prior->unit);
    out->signal = prior->signal;
    out->quality = 1000.0 + (double)prior->support_count;
    out->bitmask = field_bitmask(out->start_bit, out->bit_length, out->byte_order);
}

static void fill_export_from_guess(disc_export_signal_t *out,
                                   const char *name,
                                   disc_signal_t signal,
                                   const disc_raw_guess_t *guess,
                                   double quality) {
    snprintf(out->name, sizeof(out->name), "%s", name);
    out->start_bit = guess->start_bit;
    out->bit_length = guess->bit_length;
    out->byte_order = guess->byte_order;
    out->sign_char = guess->sign_char;
    snprintf(out->factor, sizeof(out->factor), "1");
    snprintf(out->offset, sizeof(out->offset), "0");
    snprintf(out->min_val, sizeof(out->min_val), "%d", guess->raw_min);
    snprintf(out->max_val, sizeof(out->max_val), "%d", guess->raw_max);
    out->unit[0] = '\0';
    out->signal = signal;
    out->quality = quality;
    out->bitmask = guess->bitmask;
}

static void fill_export_from_candidate(disc_export_signal_t *out,
                                       const char *name,
                                       disc_signal_t signal,
                                       const disc_candidate_t *cand) {
    int start_bit, bit_length, byte_order;
    char sign_char;
    dbc_signal_layout(cand, &start_bit, &bit_length, &byte_order, &sign_char);

    snprintf(out->name, sizeof(out->name), "%s", name);
    out->start_bit = start_bit;
    out->bit_length = bit_length;
    out->byte_order = byte_order;
    out->sign_char = sign_char;
    snprintf(out->factor, sizeof(out->factor), "1");
    snprintf(out->offset, sizeof(out->offset), "0");
    snprintf(out->min_val, sizeof(out->min_val), "%d", cand->raw_min);
    snprintf(out->max_val, sizeof(out->max_val), "%d", cand->raw_max);
    out->unit[0] = '\0';
    out->signal = signal;
    out->quality = 100.0 + cand->score;
    out->bitmask = field_bitmask(out->start_bit, out->bit_length, out->byte_order);
}

static int build_export_signals(disc_signal_t signal,
                                const disc_candidate_t *cand,
                                const disc_prior_match_t *prior_match,
                                const disc_raw_phase_t *baseline_raw,
                                const disc_raw_phase_t *active_raw,
                                disc_export_signal_t *signals,
                                int max_signals) {
    if (!cand || !signals || max_signals <= 0) return 0;

    if (prior_match && prior_match->matched) {
        fill_export_from_prior(&signals[0], &prior_match->prior);
        return 1;
    }

    if (signal == DISC_SIG_WHEEL_SPEED && max_signals >= 4) {
        disc_raw_guess_t guesses[4];
        int guess_count = best_multi_raw_guesses(signal, baseline_raw, active_raw,
                                                 cand, guesses, 4);
        if (guess_count > 1) {
            for (int i = 0; i < guess_count; i++) {
                char name[DISC_PRIOR_NAME_LEN];
                snprintf(name, sizeof(name), "WheelSpeed_%d", i + 1);
                fill_export_from_guess(&signals[i], name, signal, &guesses[i],
                                       500.0 + guesses[i].score +
                                       raw_guess_candidate_bonus(cand, &guesses[i]));
            }
            return guess_count;
        }
    }

    disc_raw_guess_t guess;
    if (best_raw_guess(signal, baseline_raw, active_raw, cand, &guess) &&
        guess.score >= 8.0) {
        fill_export_from_guess(&signals[0], signal_dbc_name(signal), signal, &guess,
                               500.0 + guess.score +
                               raw_guess_candidate_bonus(cand, &guess));
        return 1;
    }

    fill_export_from_candidate(&signals[0], signal_dbc_name(signal), signal, cand);
    return 1;
}

static int cmp_int(const void *a, const void *b) {
    int ia = *(const int *)a;
    int ib = *(const int *)b;
    if (ia < ib) return -1;
    if (ia > ib) return 1;
    return 0;
}

static int cmp_export_quality_desc(const void *a, const void *b) {
    const disc_export_signal_t *ea = (const disc_export_signal_t *)a;
    const disc_export_signal_t *eb = (const disc_export_signal_t *)b;
    if (ea->quality > eb->quality) return -1;
    if (ea->quality < eb->quality) return 1;
    if (ea->start_bit < eb->start_bit) return -1;
    if (ea->start_bit > eb->start_bit) return 1;
    return 0;
}

static int cmp_export_start_asc(const void *a, const void *b) {
    const disc_export_signal_t *ea = (const disc_export_signal_t *)a;
    const disc_export_signal_t *eb = (const disc_export_signal_t *)b;
    if (ea->start_bit < eb->start_bit) return -1;
    if (ea->start_bit > eb->start_bit) return 1;
    return 0;
}

static int dbc_big_endian_next_bit(int bit) {
    return (bit % 8 == 0) ? bit + 15 : bit - 1;
}

static int bytes_from_mask(const bool mask[DISC_MAX_DLC]) {
    int count = 0;
    for (int i = 0; i < DISC_MAX_DLC; i++) {
        if (mask[i]) count++;
    }
    return count;
}

static void candidate_byte_mask(const disc_candidate_t *cand, bool mask[DISC_MAX_DLC]) {
    memset(mask, 0, DISC_MAX_DLC * sizeof(mask[0]));
    if (!cand) return;

    if (cand->byte_idx >= 0 && cand->byte_idx < DISC_MAX_DLC) mask[cand->byte_idx] = true;
    if (cand->byte2_idx >= 0 && cand->byte2_idx < DISC_MAX_DLC) mask[cand->byte2_idx] = true;
}

static void prior_byte_mask(const disc_dbc_prior_t *prior, bool mask[DISC_MAX_DLC]) {
    memset(mask, 0, DISC_MAX_DLC * sizeof(mask[0]));
    if (!prior) return;

    int bit = prior->start_bit;
    for (int i = 0; i < prior->bit_length; i++) {
        int byte_idx = bit / 8;
        if (byte_idx >= 0 && byte_idx < DISC_MAX_DLC) mask[byte_idx] = true;
        bit = (prior->byte_order == 1) ? (bit + 1) : dbc_big_endian_next_bit(bit);
    }
}

static int overlap_byte_count(const bool a[DISC_MAX_DLC], const bool b[DISC_MAX_DLC]) {
    int overlap = 0;
    for (int i = 0; i < DISC_MAX_DLC; i++) {
        if (a[i] && b[i]) overlap++;
    }
    return overlap;
}

static int score_dbc_prior(disc_signal_t signal,
                           const disc_candidate_t *cand,
                           const disc_dbc_prior_t *prior) {
    if (!cand || !prior) return -1;
    if (prior->signal != signal) return -1;
    if (prior->can_id != cand->can_id) return -1;

    bool cand_mask[DISC_MAX_DLC];
    bool prior_mask[DISC_MAX_DLC];
    candidate_byte_mask(cand, cand_mask);
    prior_byte_mask(prior, prior_mask);

    int cand_bytes = bytes_from_mask(cand_mask);
    int prior_bytes = bytes_from_mask(prior_mask);
    int overlap = overlap_byte_count(cand_mask, prior_mask);
    if (cand_bytes == 0 || prior_bytes == 0 || overlap == 0) return -1;

    bool cand_word = cand->byte2_idx >= 0 && abs(cand->byte_idx - cand->byte2_idx) == 1;
    bool prior_word = prior->bit_length > 8 || prior_bytes > 1;

    int score = 60;
    if (cand->dlc > 0 && prior->dlc == cand->dlc) score += 6;

    if (overlap == cand_bytes && overlap == prior_bytes) {
        score += 28;
    } else if (overlap == cand_bytes) {
        score += 18;
    } else {
        score += overlap * 8;
    }

    if (cand_word == prior_word) {
        score += 10;
    } else if (cand_word && prior_bytes == 2) {
        score += 4;
    } else {
        score -= 4;
    }

    if (cand_word && cand->endianness != DISC_ENDIAN_UNKNOWN) {
        int cand_order = (cand->endianness == DISC_ENDIAN_BIG) ? 0 : 1;
        score += (prior->byte_order == cand_order) ? 8 : -4;
    }

    if (cand->signedness != DISC_SIGN_UNKNOWN) {
        char cand_sign = (cand->signedness == DISC_SIGN_SIGNED) ? '-' : '+';
        score += (prior->sign_char == cand_sign) ? 4 : -2;
    }

    score += (prior->support_count > 12) ? 12 : prior->support_count;
    return score;
}

static bool find_best_dbc_prior(disc_signal_t signal,
                                const disc_candidate_t *cand,
                                const disc_dbc_prior_t *priors,
                                int prior_count,
                                disc_prior_match_t *match_out) {
    if (!cand || !priors || prior_count <= 0 || !match_out) return false;

    int best_idx = -1;
    int best_score = -1;
    for (int i = 0; i < prior_count; i++) {
        int score = score_dbc_prior(signal, cand, &priors[i]);
        if (score > best_score) {
            best_score = score;
            best_idx = i;
        }
    }

    if (best_idx < 0 || best_score < 90) return false;
    match_out->matched = true;
    match_out->prior = priors[best_idx];
    return true;
}

static uint64_t field_bitmask(int start_bit, int bit_length, int byte_order) {
    uint64_t mask = 0;
    int bit = start_bit;
    for (int i = 0; i < bit_length; i++) {
        if (bit >= 0 && bit < 64) mask |= (1ULL << bit);
        bit = (byte_order == 1) ? (bit + 1) : dbc_big_endian_next_bit(bit);
    }
    return mask;
}

static bool field_fits_dlc(int start_bit, int bit_length, int byte_order, int dlc) {
    if (start_bit < 0 || bit_length <= 0 || dlc <= 0 || dlc > DISC_MAX_DLC) return false;

    int bit = start_bit;
    for (int i = 0; i < bit_length; i++) {
        int byte_idx = bit / 8;
        if (byte_idx < 0 || byte_idx >= dlc) return false;
        bit = (byte_order == 1) ? (bit + 1) : dbc_big_endian_next_bit(bit);
    }
    return true;
}

static bool field_within_byte_bounds(int start_bit, int bit_length, int byte_order,
                                     int byte_lo, int byte_hi) {
    int bit = start_bit;
    for (int i = 0; i < bit_length; i++) {
        int byte_idx = bit / 8;
        if (byte_idx < byte_lo || byte_idx > byte_hi) return false;
        bit = (byte_order == 1) ? (bit + 1) : dbc_big_endian_next_bit(bit);
    }
    return true;
}

static bool extract_dbc_unsigned(const can_frame_t *frame,
                                 int start_bit, int bit_length, int byte_order,
                                 uint32_t *value_out) {
    if (!frame || !value_out) return false;
    if (!field_fits_dlc(start_bit, bit_length, byte_order, frame->dlc)) return false;

    uint32_t value = 0;
    if (byte_order == 1) {
        int bit = start_bit;
        for (int i = 0; i < bit_length; i++, bit++) {
            uint8_t data_bit = (frame->data[bit / 8] >> (bit % 8)) & 0x1;
            value |= ((uint32_t)data_bit << i);
        }
    } else {
        int bit = start_bit;
        for (int i = 0; i < bit_length; i++) {
            uint8_t data_bit = (frame->data[bit / 8] >> (bit % 8)) & 0x1;
            value = (value << 1) | data_bit;
            bit = dbc_big_endian_next_bit(bit);
        }
    }

    *value_out = value;
    return true;
}

static int32_t sign_extend_value(uint32_t value, int bit_length) {
    if (bit_length <= 0 || bit_length >= 32) return (int32_t)value;
    uint32_t sign_bit = 1u << (bit_length - 1);
    uint32_t full_mask = (1u << bit_length) - 1u;
    value &= full_mask;
    if (value & sign_bit) value |= ~full_mask;
    return (int32_t)value;
}

static double span_magnitude_score(int32_t span) {
    uint32_t u_span;
    int bits = 0;

    if (span <= 0) return 0.0;
    u_span = (uint32_t)span;
    while (u_span != 0) {
        bits++;
        u_span >>= 1;
    }
    return (double)bits;
}

static void signal_search_lengths(disc_signal_t signal,
                                  const int **lengths_out,
                                  int *length_count_out) {
    static const int k_steering[] = { 10, 12, 14, 16 };
    static const int k_rpm[] = { 12, 14, 16 };
    static const int k_throttle[] = { 8, 10, 12, 16 };
    static const int k_brake[] = { 8, 10, 12, 16, 1 };
    static const int k_gear[] = { 3, 4, 8, 16 };
    static const int k_wheel[] = { 12, 14, 15, 16 };

    switch (signal) {
    case DISC_SIG_STEERING:
        *lengths_out = k_steering;
        *length_count_out = (int)(sizeof(k_steering) / sizeof(k_steering[0]));
        break;
    case DISC_SIG_RPM:
        *lengths_out = k_rpm;
        *length_count_out = (int)(sizeof(k_rpm) / sizeof(k_rpm[0]));
        break;
    case DISC_SIG_THROTTLE:
        *lengths_out = k_throttle;
        *length_count_out = (int)(sizeof(k_throttle) / sizeof(k_throttle[0]));
        break;
    case DISC_SIG_BRAKE:
        *lengths_out = k_brake;
        *length_count_out = (int)(sizeof(k_brake) / sizeof(k_brake[0]));
        break;
    case DISC_SIG_GEAR:
        *lengths_out = k_gear;
        *length_count_out = (int)(sizeof(k_gear) / sizeof(k_gear[0]));
        break;
    case DISC_SIG_WHEEL_SPEED:
    default:
        *lengths_out = k_wheel;
        *length_count_out = (int)(sizeof(k_wheel) / sizeof(k_wheel[0]));
        break;
    }
}

static bool signal_allows_signed_guess(disc_signal_t signal) {
    return signal == DISC_SIG_STEERING || signal == DISC_SIG_BRAKE;
}

static void guess_search_byte_bounds(disc_signal_t signal,
                                     const disc_candidate_t *cand,
                                     int *byte_lo_out,
                                     int *byte_hi_out) {
    if (!cand || cand->byte_idx < 0) {
        *byte_lo_out = 0;
        *byte_hi_out = DISC_MAX_DLC - 1;
        return;
    }

    if (signal == DISC_SIG_WHEEL_SPEED) {
        *byte_lo_out = 0;
        *byte_hi_out = cand->dlc > 0 ? cand->dlc - 1 : DISC_MAX_DLC - 1;
        return;
    }

    int byte_lo = cand->byte_idx;
    int byte_hi = cand->byte_idx;
    if (cand->byte2_idx >= 0) {
        if (cand->byte2_idx < byte_lo) byte_lo = cand->byte2_idx;
        if (cand->byte2_idx > byte_hi) byte_hi = cand->byte2_idx;
    } else if (signal != DISC_SIG_GEAR) {
        if (byte_lo > 0) byte_lo--;
        if (byte_hi + 1 < cand->dlc) byte_hi++;
    }

    *byte_lo_out = byte_lo;
    *byte_hi_out = byte_hi;
}

static double raw_guess_family_bonus(disc_signal_t signal,
                                     int bit_length,
                                     char sign_char,
                                     int32_t raw_min,
                                     int32_t raw_max,
                                     int active_unique,
                                     int baseline_unique) {
    double score = 0.0;

    switch (signal) {
    case DISC_SIG_STEERING:
        if (bit_length >= 12) score += 4.0;
        if (sign_char == '-' && raw_min < 0 && raw_max > 0) score += 8.0;
        if ((raw_max - raw_min) > 64) score += 3.0;
        break;
    case DISC_SIG_RPM:
        if (bit_length >= 12) score += 6.0;
        if (sign_char == '+') score += 2.0;
        if ((raw_max - raw_min) > 300) score += 3.0;
        break;
    case DISC_SIG_THROTTLE:
        if (bit_length == 8 || bit_length == 10 || bit_length == 12 || bit_length == 16) score += 4.0;
        if (sign_char == '+') score += 1.0;
        break;
    case DISC_SIG_BRAKE:
        if (bit_length == 1) score -= 30.0;
        if (bit_length == 10) score += 8.0;
        else if (bit_length == 12) score += 6.0;
        else if (bit_length == 8) score += 4.0;
        else if (bit_length == 16) score += 1.0;
        if (active_unique >= 4 && baseline_unique <= 2) score += 4.0;
        break;
    case DISC_SIG_GEAR:
        if (bit_length == 3 || bit_length == 4 || bit_length == 8) score += 5.0;
        if (active_unique >= 3 && active_unique <= 8) score += 10.0;
        if (baseline_unique <= 2) score += 4.0;
        break;
    case DISC_SIG_WHEEL_SPEED:
        if (bit_length == 12 || bit_length == 14 || bit_length == 15 || bit_length == 16) score += 8.0;
        if (bit_length == 16) {
            score += 8.0;
        } else if (bit_length == 15) {
            score += 4.0;
        } else if (bit_length == 14) {
            score += 2.0;
        }
        if (sign_char == '+') score += 2.0;
        if (baseline_unique <= 2) score += 6.0;
        break;
    default:
        break;
    }

    if (bit_length < 4 && signal != DISC_SIG_GEAR) score -= 20.0;
    return score;
}

static bool evaluate_raw_guess(disc_signal_t signal,
                               const disc_raw_phase_t *baseline_raw,
                               const disc_raw_phase_t *active_raw,
                               uint32_t can_id,
                               int start_bit,
                               int bit_length,
                               int byte_order,
                               char sign_char,
                               disc_raw_guess_t *guess_out) {
    if (!active_raw || !active_raw->frames || active_raw->frame_count == 0 || !guess_out) return false;

    uint8_t seen_active[1u << 16] = {0};
    uint8_t seen_base[1u << 16] = {0};
    bool have_active = false;
    bool have_base = false;
    int32_t active_min = 0, active_max = 0;
    int32_t base_min = 0, base_max = 0;
    int active_unique = 0;
    int base_unique = 0;
    bool have_prev = false;
    uint32_t prev_u = 0;
    int changed = 0;
    int plus_one = 0;
    uint64_t mask = field_bitmask(start_bit, bit_length, byte_order);
    int touched_bytes[DISC_MAX_DLC];
    int touched_count = 0;
    for (int byte_idx = 0; byte_idx < DISC_MAX_DLC; byte_idx++) {
        if ((mask & (0xFFULL << (byte_idx * 8))) != 0) {
            touched_bytes[touched_count++] = byte_idx;
        }
    }

    bool compare_byte_significance = false;
    int msb_byte_idx = -1;
    int lsb_byte_idx = -1;
    uint8_t seen_msb_byte[256] = {0};
    uint8_t seen_lsb_byte[256] = {0};
    int msb_byte_unique = 0;
    int lsb_byte_unique = 0;
    if (touched_count == 2 && touched_bytes[1] == touched_bytes[0] + 1 && bit_length > 8) {
        compare_byte_significance = true;
        if (byte_order == 1) {
            lsb_byte_idx = touched_bytes[0];
            msb_byte_idx = touched_bytes[1];
        } else {
            msb_byte_idx = touched_bytes[0];
            lsb_byte_idx = touched_bytes[1];
        }
    }

    for (int i = 0; i < active_raw->frame_count; i++) {
        const can_frame_t *frame = &active_raw->frames[i];
        if (frame->can_id != can_id) continue;

        uint32_t u_value;
        if (!extract_dbc_unsigned(frame, start_bit, bit_length, byte_order, &u_value)) continue;

        int32_t s_value = (sign_char == '-') ? sign_extend_value(u_value, bit_length)
                                             : (int32_t)u_value;
        if (!have_active) {
            active_min = active_max = s_value;
        } else {
            if (s_value < active_min) active_min = s_value;
            if (s_value > active_max) active_max = s_value;
        }
        have_active = true;

        if (!seen_active[u_value & 0xFFFFu]) {
            seen_active[u_value & 0xFFFFu] = 1;
            active_unique++;
        }

        if (have_prev) {
            if (u_value != prev_u) {
                changed++;
                if (((u_value - prev_u) & ((1u << bit_length) - 1u)) == 1u) plus_one++;
            }
        }
        prev_u = u_value;
        have_prev = true;

        if (compare_byte_significance &&
            frame->dlc > msb_byte_idx && frame->dlc > lsb_byte_idx) {
            uint8_t msb_byte = frame->data[msb_byte_idx];
            uint8_t lsb_byte = frame->data[lsb_byte_idx];
            if (!seen_msb_byte[msb_byte]) {
                seen_msb_byte[msb_byte] = 1;
                msb_byte_unique++;
            }
            if (!seen_lsb_byte[lsb_byte]) {
                seen_lsb_byte[lsb_byte] = 1;
                lsb_byte_unique++;
            }
        }
    }

    if (!have_active) return false;

    if (baseline_raw && baseline_raw->frames) {
        for (int i = 0; i < baseline_raw->frame_count; i++) {
            const can_frame_t *frame = &baseline_raw->frames[i];
            if (frame->can_id != can_id) continue;

            uint32_t u_value;
            if (!extract_dbc_unsigned(frame, start_bit, bit_length, byte_order, &u_value)) continue;

            int32_t s_value = (sign_char == '-') ? sign_extend_value(u_value, bit_length)
                                                 : (int32_t)u_value;
            if (!have_base) {
                base_min = base_max = s_value;
            } else {
                if (s_value < base_min) base_min = s_value;
                if (s_value > base_max) base_max = s_value;
            }
            have_base = true;

            if (!seen_base[u_value & 0xFFFFu]) {
                seen_base[u_value & 0xFFFFu] = 1;
                base_unique++;
            }
        }
    }

    int32_t active_span = active_max - active_min;
    int32_t base_span = have_base ? (base_max - base_min) : 0;
    double score = 0.0;
    double active_span_score = span_magnitude_score(active_span);
    double base_span_score = span_magnitude_score(base_span);
    if (active_span_score > base_span_score) {
        score += (active_span_score - base_span_score) * 2.0;
    }
    if (active_unique > base_unique) score += (double)(active_unique - base_unique) * 0.5;

    if (changed >= 8 && active_unique >= 8) {
        double inc_ratio = (double)plus_one / (double)changed;
        if (inc_ratio >= 0.75) score -= 100.0;
    }

    if (compare_byte_significance) {
        if (lsb_byte_unique > msb_byte_unique) score += 20.0;
        else if (lsb_byte_unique < msb_byte_unique) score -= 20.0;
    }

    score += raw_guess_family_bonus(signal, bit_length, sign_char,
                                    active_min, active_max,
                                    active_unique, base_unique);

    guess_out->start_bit = start_bit;
    guess_out->bit_length = bit_length;
    guess_out->byte_order = byte_order;
    guess_out->sign_char = sign_char;
    guess_out->raw_min = active_min;
    guess_out->raw_max = active_max;
    guess_out->active_unique = active_unique;
    guess_out->baseline_unique = base_unique;
    guess_out->score = score;
    guess_out->bitmask = mask;
    return true;
}

static int cmp_raw_guess_score_desc(const void *a, const void *b) {
    const disc_raw_guess_t *ga = (const disc_raw_guess_t *)a;
    const disc_raw_guess_t *gb = (const disc_raw_guess_t *)b;
    if (ga->score > gb->score) return -1;
    if (ga->score < gb->score) return 1;
    return 0;
}

static int cmp_raw_guess_start_asc(const void *a, const void *b) {
    const disc_raw_guess_t *ga = (const disc_raw_guess_t *)a;
    const disc_raw_guess_t *gb = (const disc_raw_guess_t *)b;
    if (ga->start_bit < gb->start_bit) return -1;
    if (ga->start_bit > gb->start_bit) return 1;
    return 0;
}

static int collect_raw_guesses(disc_signal_t signal,
                               const disc_raw_phase_t *baseline_raw,
                               const disc_raw_phase_t *active_raw,
                               const disc_candidate_t *cand,
                               disc_raw_guess_t *guesses,
                               int max_guesses) {
    if (!active_raw || !cand || !guesses || max_guesses <= 0) return 0;

    int byte_lo, byte_hi;
    guess_search_byte_bounds(signal, cand, &byte_lo, &byte_hi);
    if (byte_lo < 0) byte_lo = 0;
    if (byte_hi >= cand->dlc) byte_hi = cand->dlc - 1;

    const int *lengths = NULL;
    int length_count = 0;
    signal_search_lengths(signal, &lengths, &length_count);

    int count = 0;
    int max_start = cand->dlc * 8;
    for (int li = 0; li < length_count; li++) {
        int bit_length = lengths[li];
        int order_lo = 0;
        int order_hi = 1;
        if (bit_length <= 8) {
            order_lo = 1;
            order_hi = 1;
        }

        for (int byte_order = order_lo; byte_order <= order_hi; byte_order++) {
            if (bit_length <= 8 && byte_order == 1) {
                /* Single-byte signals do not need both byte orders. */
            }

            int sign_variants = signal_allows_signed_guess(signal) ? 2 : 1;

            for (int si = 0; si < sign_variants; si++) {
                char sign_char = (si == 1) ? '-' : '+';
                for (int start_bit = 0; start_bit < max_start; start_bit++) {
                    if (!field_fits_dlc(start_bit, bit_length, byte_order, cand->dlc)) continue;
                    if (!field_within_byte_bounds(start_bit, bit_length, byte_order,
                                                  byte_lo, byte_hi)) continue;

                    disc_raw_guess_t guess;
                    if (!evaluate_raw_guess(signal, baseline_raw, active_raw, cand->can_id,
                                            start_bit, bit_length, byte_order, sign_char,
                                            &guess)) {
                        continue;
                    }

                    if (guess.score <= 0.0) continue;
                    if (count < max_guesses) {
                        guesses[count++] = guess;
                    }
                }
            }
        }
    }

    qsort(guesses, (size_t)count, sizeof(*guesses), cmp_raw_guess_score_desc);
    return count;
}

static double raw_guess_candidate_bonus(const disc_candidate_t *cand,
                                        const disc_raw_guess_t *guess) {
    if (!cand || !guess) return 0.0;

    double bonus = 0.0;
    uint64_t cand_mask = 0;
    if (cand->byte_idx >= 0 && cand->byte_idx < DISC_MAX_DLC)
        cand_mask |= (0xFFULL << (cand->byte_idx * 8));
    if (cand->byte2_idx >= 0 && cand->byte2_idx < DISC_MAX_DLC)
        cand_mask |= (0xFFULL << (cand->byte2_idx * 8));

    if (cand_mask != 0) {
        if ((guess->bitmask & ~cand_mask) == 0) bonus += 6.0;
        if ((guess->bitmask & cand_mask) != 0) bonus += 4.0;
    }

    if (cand->byte2_idx >= 0 && cand->endianness != DISC_ENDIAN_UNKNOWN) {
        int cand_order = (cand->endianness == DISC_ENDIAN_BIG) ? 0 : 1;
        bonus += (guess->byte_order == cand_order) ? 3.0 : -1.0;
    }

    if (cand->signedness != DISC_SIGN_UNKNOWN) {
        char cand_sign = (cand->signedness == DISC_SIGN_SIGNED) ? '-' : '+';
        bonus += (guess->sign_char == cand_sign) ? 2.0 : -1.0;
    }

    return bonus;
}

static int collect_wheel_pair_guesses(const disc_raw_phase_t *baseline_raw,
                                      const disc_raw_phase_t *active_raw,
                                      const disc_candidate_t *cand,
                                      disc_raw_guess_t *guesses,
                                      int max_guesses) {
    if (!cand || !guesses || max_guesses <= 0) return 0;

    const int *lengths = NULL;
    int length_count = 0;
    signal_search_lengths(DISC_SIG_WHEEL_SPEED, &lengths, &length_count);

    int count = 0;
    for (int byte_idx = 0; byte_idx + 1 < cand->dlc; byte_idx += 2) {
        for (int li = 0; li < length_count; li++) {
            int bit_length = lengths[li];
            int order_lo = 0;
            int order_hi = 1;

            for (int byte_order = order_lo; byte_order <= order_hi; byte_order++) {
                int start_bit = (byte_order == 1) ? (byte_idx * 8) : (byte_idx * 8 + 7);
                disc_raw_guess_t guess;
                if (!evaluate_raw_guess(DISC_SIG_WHEEL_SPEED, baseline_raw, active_raw,
                                        cand->can_id, start_bit, bit_length,
                                        byte_order, '+', &guess)) {
                    continue;
                }
                if (guess.score <= 0.0) continue;
                if (count < max_guesses) guesses[count++] = guess;
            }
        }
    }

    qsort(guesses, (size_t)count, sizeof(*guesses), cmp_raw_guess_score_desc);
    return count;
}

static bool best_raw_guess(disc_signal_t signal,
                           const disc_raw_phase_t *baseline_raw,
                           const disc_raw_phase_t *active_raw,
                           const disc_candidate_t *cand,
                           disc_raw_guess_t *guess_out) {
    disc_raw_guess_t guesses[256];
    int guess_count = collect_raw_guesses(signal, baseline_raw, active_raw, cand,
                                          guesses, (int)(sizeof(guesses) / sizeof(guesses[0])));
    if (guess_count <= 0) return false;

    int best_idx = 0;
    double best_score = guesses[0].score + raw_guess_candidate_bonus(cand, &guesses[0]);
    for (int i = 1; i < guess_count; i++) {
        double score = guesses[i].score + raw_guess_candidate_bonus(cand, &guesses[i]);
        if (score > best_score) {
            best_score = score;
            best_idx = i;
        }
    }

    *guess_out = guesses[best_idx];
    return true;
}

static int best_multi_raw_guesses(disc_signal_t signal,
                                  const disc_raw_phase_t *baseline_raw,
                                  const disc_raw_phase_t *active_raw,
                                  const disc_candidate_t *cand,
                                  disc_raw_guess_t *guess_out,
                                  int max_guess_out) {
    disc_raw_guess_t guesses[256];
    int guess_count;
    if (signal == DISC_SIG_WHEEL_SPEED) {
        guess_count = collect_wheel_pair_guesses(baseline_raw, active_raw, cand,
                                                 guesses,
                                                 (int)(sizeof(guesses) / sizeof(guesses[0])));
    } else {
        guess_count = collect_raw_guesses(signal, baseline_raw, active_raw, cand,
                                          guesses,
                                          (int)(sizeof(guesses) / sizeof(guesses[0])));
    }
    int chosen = 0;

    if (signal == DISC_SIG_WHEEL_SPEED && guess_count > 0) {
        int combo_bit_length = 0;
        int combo_byte_order = 1;
        int combo_count_best = -1;
        double combo_score_best = 0.0;

        for (int i = 0; i < guess_count; i++) {
            int combo_count = 0;
            double combo_score = 0.0;
            uint64_t used_masks = 0;

            for (int j = 0; j < guess_count; j++) {
                if (guesses[j].bit_length != guesses[i].bit_length) continue;
                if (guesses[j].byte_order != guesses[i].byte_order) continue;
                if ((guesses[j].bitmask & used_masks) != 0) continue;
                used_masks |= guesses[j].bitmask;
                combo_count++;
                combo_score += guesses[j].score;
            }

            if (combo_count > combo_count_best ||
                (combo_count == combo_count_best && combo_score > combo_score_best)) {
                combo_count_best = combo_count;
                combo_score_best = combo_score;
                combo_bit_length = guesses[i].bit_length;
                combo_byte_order = guesses[i].byte_order;
            }
        }

        for (int i = 0; i < guess_count && chosen < max_guess_out; i++) {
            if (guesses[i].bit_length != combo_bit_length) continue;
            if (guesses[i].byte_order != combo_byte_order) continue;
            if (guesses[i].score < 12.0) continue;

            bool overlaps = false;
            for (int j = 0; j < chosen; j++) {
                if ((guesses[i].bitmask & guess_out[j].bitmask) != 0) {
                    overlaps = true;
                    break;
                }
            }
            if (overlaps) continue;
            guess_out[chosen++] = guesses[i];
        }

        qsort(guess_out, (size_t)chosen, sizeof(*guess_out), cmp_raw_guess_start_asc);
        return chosen;
    }

    while (chosen < max_guess_out) {
        int best_idx = -1;
        double best_score = 0.0;

        for (int i = 0; i < guess_count; i++) {
            double score = guesses[i].score + raw_guess_candidate_bonus(cand, &guesses[i]);
            if (score < 12.0) continue;

            bool overlaps = false;
            for (int j = 0; j < chosen; j++) {
                if ((guesses[i].bitmask & guess_out[j].bitmask) != 0) {
                    overlaps = true;
                    break;
                }
            }
            if (overlaps) continue;

            if (best_idx < 0 || score > best_score) {
                best_idx = i;
                best_score = score;
            }
        }

        if (best_idx < 0) break;
        guess_out[chosen++] = guesses[best_idx];
        guesses[best_idx].score = -1e9;
    }

    qsort(guess_out, (size_t)chosen, sizeof(*guess_out), cmp_raw_guess_start_asc);
    return chosen;
}

static const disc_id_t *find_phase_id(const disc_phase_t *phase, uint32_t can_id) {
    if (!phase) return NULL;
    for (int i = 0; i < phase->id_count; i++) {
        if (phase->ids[i].can_id == can_id) return &phase->ids[i];
    }
    return NULL;
}

static int estimate_cycle_ms_from_phase(const disc_phase_t *phase, uint32_t can_id) {
    const disc_id_t *id = find_phase_id(phase, can_id);
    if (!id || phase->duration_s <= 0 || id->frame_count <= 0) return -1;
    double hz = (double)id->frame_count / phase->duration_s;
    if (hz <= 0) return -1;
    return (int)(1000.0 / hz + 0.5);
}

static int compute_cycle_ms_from_raw(const disc_raw_phase_t *raw_phase, uint32_t can_id) {
    if (!raw_phase || raw_phase->frame_count < 2 || !raw_phase->frames) return -1;

    int *intervals = (int *)malloc((size_t)raw_phase->frame_count * sizeof(*intervals));
    if (!intervals) return -1;

    int interval_count = 0;
    int64_t prev_ts = -1;
    for (int i = 0; i < raw_phase->frame_count; i++) {
        const can_frame_t *frame = &raw_phase->frames[i];
        if (frame->can_id != can_id) continue;
        if (frame->timestamp_ms <= 0) continue;
        if (prev_ts > 0) {
            int delta = (int)(frame->timestamp_ms - prev_ts);
            if (delta > 0 && delta < 10000) {
                intervals[interval_count++] = delta;
            }
        }
        prev_ts = frame->timestamp_ms;
    }

    if (interval_count == 0) {
        free(intervals);
        return -1;
    }

    qsort(intervals, (size_t)interval_count, sizeof(*intervals), cmp_int);
    int cycle_ms;
    if (interval_count % 2 == 1) {
        cycle_ms = intervals[interval_count / 2];
    } else {
        cycle_ms = (intervals[interval_count / 2 - 1] + intervals[interval_count / 2]) / 2;
    }

    free(intervals);
    return cycle_ms;
}

static int estimate_message_cycle_ms(uint32_t can_id,
                                     const disc_phase_t phases[DISC_NUM_PHASES],
                                     const disc_raw_phase_t raw_phases[DISC_NUM_PHASES],
                                     const disc_result_t *result) {
    int cycle_ms = compute_cycle_ms_from_raw(&raw_phases[DISC_PHASE_BASELINE], can_id);
    if (cycle_ms > 0) return cycle_ms;

    cycle_ms = estimate_cycle_ms_from_phase(&phases[DISC_PHASE_BASELINE], can_id);
    if (cycle_ms > 0) return cycle_ms;

    for (int s = 0; s < DISC_SIG_COUNT; s++) {
        if (!result->found[s]) continue;
        if (result->signals[s].can_id != can_id) continue;

        int phase_idx = signal_phase_index((disc_signal_t)s);
        cycle_ms = compute_cycle_ms_from_raw(&raw_phases[phase_idx], can_id);
        if (cycle_ms > 0) return cycle_ms;

        cycle_ms = estimate_cycle_ms_from_phase(&phases[phase_idx], can_id);
        if (cycle_ms > 0) return cycle_ms;

        if (result->signals[s].hz > 0) {
            return (int)(1000.0 / result->signals[s].hz + 0.5);
        }
    }

    return -1;
}

static void dbc_signal_layout(const disc_candidate_t *cand,
                              int *start_bit, int *bit_length,
                              int *byte_order, char *sign_char) {
    bool is_word = cand->byte2_idx >= 0 && abs(cand->byte_idx - cand->byte2_idx) == 1;

    *sign_char = (cand->signedness == DISC_SIGN_SIGNED) ? '-' : '+';

    if (!is_word) {
        *start_bit = cand->byte_idx * 8;
        *bit_length = 8;
        *byte_order = 1;
        return;
    }

    *bit_length = 16;
    if (cand->endianness == DISC_ENDIAN_BIG) {
        *start_bit = cand->byte_idx * 8 + 7;
        *byte_order = 0;
    } else {
        *start_bit = cand->byte2_idx * 8;
        *byte_order = 1;
    }
}

static int collect_gear_values(const disc_raw_phase_t *raw_phase,
                               uint32_t can_id,
                               const disc_export_signal_t *sig,
                               int values[16]) {
    if (!raw_phase || !raw_phase->frames || raw_phase->frame_count == 0) return 0;
    if (!sig) return 0;

    int count = 0;
    for (int i = 0; i < raw_phase->frame_count; i++) {
        const can_frame_t *frame = &raw_phase->frames[i];
        if (frame->can_id != can_id) continue;

        uint32_t raw_u = 0;
        if (!extract_dbc_unsigned(frame, sig->start_bit, sig->bit_length,
                                  sig->byte_order, &raw_u)) {
            continue;
        }

        int value = (sig->sign_char == '-') ? sign_extend_value(raw_u, sig->bit_length)
                                            : (int)raw_u;
        bool seen = false;
        for (int j = 0; j < count; j++) {
            if (values[j] == value) {
                seen = true;
                break;
            }
        }
        if (seen) continue;
        if (count < 16) {
            values[count++] = value;
        } else {
            return 0;
        }
    }

    qsort(values, (size_t)count, sizeof(*values), cmp_int);
    return count;
}

char *disc_render_draft_dbc(const disc_phase_t phases[DISC_NUM_PHASES],
                            const disc_raw_phase_t raw_phases[DISC_NUM_PHASES],
                            const disc_result_t *result,
                            size_t *length_out) {
    if (!phases || !raw_phases || !result) return NULL;

    disc_string_builder_t out = {0};
    disc_prior_match_t prior_matches[DISC_SIG_COUNT];
    memset(prior_matches, 0, sizeof(prior_matches));

    for (int s = 0; s < DISC_SIG_COUNT; s++) {
        if (!result->found[s]) continue;
        find_best_dbc_prior((disc_signal_t)s, &result->signals[s],
                            s_dbc_priors, DISC_DBC_PRIOR_COUNT, &prior_matches[s]);
    }

    uint32_t msg_ids[DISC_SIG_COUNT];
    uint8_t msg_dlcs[DISC_SIG_COUNT];
    int msg_cycles[DISC_SIG_COUNT];
    int msg_count = 0;

    for (int s = 0; s < DISC_SIG_COUNT; s++) {
        if (!result->found[s]) continue;

        uint32_t can_id = result->signals[s].can_id;
        uint8_t msg_dlc = result->signals[s].dlc;
        if (prior_matches[s].matched && prior_matches[s].prior.dlc > msg_dlc) {
            msg_dlc = prior_matches[s].prior.dlc;
        }
        int idx = -1;
        for (int i = 0; i < msg_count; i++) {
            if (msg_ids[i] == can_id) {
                idx = i;
                break;
            }
        }

        if (idx < 0) {
            idx = msg_count++;
            msg_ids[idx] = can_id;
            msg_dlcs[idx] = msg_dlc;
            msg_cycles[idx] = -1;
        } else if (msg_dlc > msg_dlcs[idx]) {
            msg_dlcs[idx] = msg_dlc;
        }
    }

    for (int i = 0; i < msg_count - 1; i++) {
        for (int j = i + 1; j < msg_count; j++) {
            if (msg_ids[j] < msg_ids[i]) {
                uint32_t tmp_id = msg_ids[i];
                uint8_t tmp_dlc = msg_dlcs[i];
                int tmp_cycle = msg_cycles[i];
                msg_ids[i] = msg_ids[j];
                msg_dlcs[i] = msg_dlcs[j];
                msg_cycles[i] = msg_cycles[j];
                msg_ids[j] = tmp_id;
                msg_dlcs[j] = tmp_dlc;
                msg_cycles[j] = tmp_cycle;
            }
        }
    }

#define OUT(...) do { if (!disc_sb_appendf(&out, __VA_ARGS__)) goto fail; } while (0)

    OUT("VERSION \"\"\n\n");
    OUT("NS_ :\n");
    OUT("\tNS_DESC_\n");
    OUT("\tBA_DEF_\n");
    OUT("\tBA_\n");
    OUT("\tVAL_\n");
    OUT("\tCAT_DEF_\n");
    OUT("\tCAT_\n");
    OUT("\tFILTER\n");
    OUT("\tBA_DEF_DEF_\n");
    OUT("\tEV_DATA_\n");
    OUT("\tENVVAR_DATA_\n");
    OUT("\tSGTYPE_\n");
    OUT("\tSGTYPE_VAL_\n");
    OUT("\tBA_DEF_SGTYPE_\n");
    OUT("\tBA_SGTYPE_\n");
    OUT("\tSIG_TYPE_REF_\n");
    OUT("\tVAL_TABLE_\n");
    OUT("\tSIG_GROUP_\n");
    OUT("\tSIGTYPE_VALTYPE_\n");
    OUT("\tSG_MUL_VAL_\n\n");
    OUT("BS_: 500\n\n");
    OUT("BU_: DISCOVER\n\n");

    bool gear_export_found = false;
    uint32_t gear_export_can_id = 0;
    disc_export_signal_t gear_export = {0};

    for (int i = 0; i < msg_count; i++) {
        OUT("BO_ %u MSG_%03X: %u DISCOVER\n",
            msg_ids[i], msg_ids[i], msg_dlcs[i]);

        disc_export_signal_t msg_candidates[DISC_SIG_COUNT * 4];
        disc_export_signal_t msg_selected[DISC_SIG_COUNT * 4];
        int msg_candidate_count = 0;
        int msg_selected_count = 0;

        for (int s = 0; s < DISC_SIG_COUNT; s++) {
            if (!result->found[s]) continue;
            const disc_candidate_t *cand = &result->signals[s];
            if (cand->can_id != msg_ids[i]) continue;

            disc_export_signal_t export_signals[4] = {0};
            int phase_idx = signal_phase_index((disc_signal_t)s);
            int export_count = build_export_signals((disc_signal_t)s, cand,
                                                    &prior_matches[s],
                                                    &raw_phases[DISC_PHASE_BASELINE],
                                                    &raw_phases[phase_idx],
                                                    export_signals,
                                                    (int)(sizeof(export_signals) /
                                                          sizeof(export_signals[0])));
            for (int e = 0; e < export_count &&
                            msg_candidate_count < (int)(sizeof(msg_candidates) /
                                                        sizeof(msg_candidates[0])); e++) {
                msg_candidates[msg_candidate_count++] = export_signals[e];
            }
        }

        qsort(msg_candidates, (size_t)msg_candidate_count,
              sizeof(msg_candidates[0]), cmp_export_quality_desc);

        for (int c = 0; c < msg_candidate_count; c++) {
            bool overlaps = false;
            for (int s = 0; s < msg_selected_count; s++) {
                if ((msg_candidates[c].bitmask & msg_selected[s].bitmask) != 0) {
                    overlaps = true;
                    break;
                }
            }
            if (overlaps) continue;
            msg_selected[msg_selected_count++] = msg_candidates[c];
        }

        qsort(msg_selected, (size_t)msg_selected_count,
              sizeof(msg_selected[0]), cmp_export_start_asc);

        for (int s = 0; s < msg_selected_count; s++) {
            const disc_export_signal_t *sig = &msg_selected[s];
            if (sig->signal == DISC_SIG_GEAR) {
                gear_export_found = true;
                gear_export_can_id = msg_ids[i];
                gear_export = *sig;
            }

            OUT(" SG_ %s : %d|%d@%d%c (%s,%s) [%s|%s] \"%s\" Vector__XXX\n",
                sig->name,
                sig->start_bit,
                sig->bit_length,
                sig->byte_order,
                sig->sign_char,
                sig->factor,
                sig->offset,
                sig->min_val,
                sig->max_val,
                sig->unit);
        }
        OUT("\n");
    }

    for (int i = 0; i < msg_count; i++) {
        msg_cycles[i] = estimate_message_cycle_ms(msg_ids[i], phases, raw_phases, result);
    }

    if (gear_export_found) {
        int values[16];
        int value_count = collect_gear_values(&raw_phases[DISC_PHASE_GEAR],
                                             gear_export_can_id,
                                             &gear_export,
                                             values);
        if (value_count > 0) {
            OUT("\nVAL_ %u %s\n",
                gear_export_can_id,
                gear_export.name);
            for (int i = 0; i < value_count; i++) {
                OUT("\t%d \"raw_0x%02X\"\n", values[i], values[i]);
            }
            OUT("\t;\n");
        }
    }

    OUT("\nBA_DEF_ BO_ \"GenMsgCycleTime\" INT 0 10000;\n\n");
    for (int i = 0; i < msg_count; i++) {
        if (msg_cycles[i] > 0) {
            OUT("BA_ \"GenMsgCycleTime\" BO_ %u %d;\n",
                msg_ids[i], msg_cycles[i]);
        }
    }

    if (!disc_sb_reserve(&out, 0)) goto fail;
    out.data[out.len] = '\0';
    if (length_out) *length_out = out.len;
    return out.data;

fail:
    free(out.data);
    if (length_out) *length_out = 0;
    return NULL;

#undef OUT
}

bool disc_write_draft_dbc(const char *path,
                          const disc_phase_t phases[DISC_NUM_PHASES],
                          const disc_raw_phase_t raw_phases[DISC_NUM_PHASES],
                          const disc_result_t *result) {
    if (!path || !phases || !raw_phases || !result) return false;

    size_t dbc_len = 0;
    char *dbc = disc_render_draft_dbc(phases, raw_phases, result, &dbc_len);
    if (!dbc) return false;

    FILE *f = fopen(path, "w");
    if (!f) {
        free(dbc);
        return false;
    }

    bool ok = fwrite(dbc, 1, dbc_len, f) == dbc_len;
    free(dbc);
    fclose(f);
    return ok;
}
