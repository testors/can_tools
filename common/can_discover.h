/**
 * @file can_discover.h
 * @brief PT-CAN Signal Auto-Discovery (Pure C, Platform Independent)
 *
 * Automates identification of key motorsport CAN signals (steering, RPM,
 * throttle, brake, gear) by comparing baseline (no-input) CAN traffic
 * against active (user-manipulating-control) captures.
 *
 * Algorithm:
 *   1. Capture baseline (engine on, no user input)
 *   2. Capture while user manipulates each control in turn
 *   3. For each CAN ID + byte, compute change score =
 *      max(0, active_range - baseline_range) +
 *      max(0, active_unique - baseline_unique) * 0.5
 *   4. Classify signals by highest-scoring CAN IDs per phase,
 *      excluding previously identified IDs
 */

#pragma once

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/*******************************************************************************
 * Constants
 ******************************************************************************/

/** Maximum CAN IDs tracked per phase */
#define DISC_MAX_IDS        256

/** Maximum CAN data bytes */
#define DISC_MAX_DLC        8

/** Maximum candidates returned per signal */
#define DISC_MAX_CANDIDATES 8

/** Number of capture phases */
#define DISC_NUM_PHASES     6

/*******************************************************************************
 * Phase Indices
 ******************************************************************************/

#define DISC_PHASE_BASELINE     0
#define DISC_PHASE_STEERING     1
#define DISC_PHASE_THROTTLE     2
#define DISC_PHASE_BRAKE        3
#define DISC_PHASE_GEAR         4
#define DISC_PHASE_WHEEL_SPEED  5

/*******************************************************************************
 * Signal Types
 ******************************************************************************/

typedef enum {
    DISC_SIG_STEERING = 0,
    DISC_SIG_RPM,
    DISC_SIG_THROTTLE,
    DISC_SIG_BRAKE,
    DISC_SIG_GEAR,
    DISC_SIG_WHEEL_SPEED,
    DISC_SIG_COUNT
} disc_signal_t;

/*******************************************************************************
 * Per-Byte Statistics
 ******************************************************************************/

/** Statistics for a single byte position within a CAN ID */
typedef struct {
    uint8_t  min_val;           /**< Minimum observed value */
    uint8_t  max_val;           /**< Maximum observed value */
    uint16_t unique_count;      /**< Number of distinct values seen */
    bool     seen[256];         /**< Bitmap of observed values */
} disc_byte_t;

/*******************************************************************************
 * Per-CAN-ID Statistics
 ******************************************************************************/

/** Statistics for a single CAN ID within a phase */
typedef struct {
    uint32_t    can_id;         /**< CAN ID */
    uint8_t     dlc;            /**< Data length code */
    uint32_t    frame_count;    /**< Total frames received */
    disc_byte_t bytes[DISC_MAX_DLC]; /**< Per-byte stats */
} disc_id_t;

/*******************************************************************************
 * Phase Capture
 ******************************************************************************/

/** Accumulated statistics for one capture phase */
typedef struct {
    disc_id_t   ids[DISC_MAX_IDS];  /**< Per-ID statistics */
    int         id_count;           /**< Number of unique CAN IDs seen */
    double      duration_s;         /**< Capture duration in seconds */
    int         total_frames;       /**< Total frames captured */
} disc_phase_t;

/*******************************************************************************
 * Confidence / Characterization Enums
 ******************************************************************************/

typedef enum {
    DISC_CONF_HIGH = 0,         /**< Preferred pattern matched */
    DISC_CONF_MEDIUM,           /**< Secondary pattern matched */
    DISC_CONF_LOW               /**< Fallback (highest score only) */
} disc_confidence_t;

typedef enum {
    DISC_ENDIAN_UNKNOWN = 0,
    DISC_ENDIAN_BIG,            /**< MSB at lower byte index */
    DISC_ENDIAN_LITTLE          /**< LSB at lower byte index */
} disc_endian_t;

typedef enum {
    DISC_SIGN_UNKNOWN = 0,
    DISC_SIGN_UNSIGNED,
    DISC_SIGN_SIGNED
} disc_sign_t;

/*******************************************************************************
 * Analysis Results
 ******************************************************************************/

/** A candidate CAN ID + byte position for a signal */
typedef struct {
    uint32_t can_id;            /**< CAN ID */
    uint8_t  dlc;               /**< DLC */
    double   hz;                /**< Estimated update rate */
    int      byte_idx;          /**< Primary byte index (-1 if none) */
    int      byte2_idx;         /**< Secondary byte index (-1 if none) */
    double   score;             /**< Change score */
    disc_confidence_t confidence; /**< Detection confidence */
    /* Characterization (filled by disc_characterize) */
    disc_endian_t endianness;   /**< Byte order (2-byte signals only) */
    disc_sign_t   signedness;   /**< Signed/unsigned */
    int32_t       raw_min;      /**< Observed minimum raw value */
    int32_t       raw_max;      /**< Observed maximum raw value */
} disc_candidate_t;

/** Final discovery results for all signals */
typedef struct {
    disc_candidate_t signals[DISC_SIG_COUNT];   /**< Best candidate per signal */
    bool             found[DISC_SIG_COUNT];     /**< Whether signal was found */
} disc_result_t;

/*******************************************************************************
 * API
 ******************************************************************************/

/**
 * @brief Initialize a phase for capturing
 * @param phase Phase structure to initialize
 */
void disc_phase_init(disc_phase_t *phase);

/**
 * @brief Add a CAN frame to phase statistics
 * @param phase Phase to accumulate into
 * @param can_id CAN ID
 * @param data Frame data bytes
 * @param dlc Data length code
 */
void disc_phase_add_frame(disc_phase_t *phase, uint32_t can_id,
                           const uint8_t *data, uint8_t dlc);

/**
 * @brief Finalize phase capture (set duration)
 * @param phase Phase to finalize
 * @param duration_s Duration of capture in seconds
 */
void disc_phase_finalize(disc_phase_t *phase, double duration_s);

/**
 * @brief Analyze all phases and classify signals
 *
 * Compares each active phase against baseline, scores CAN IDs,
 * and assigns the best candidates to each signal type.
 *
 * @param phases Array of DISC_NUM_PHASES phase captures
 *               [0]=baseline, [1]=steering, [2]=throttle, [3]=brake,
 *               [4]=gear, [5]=wheel_speed
 * @param result Output: classified signals
 */
void disc_analyze(const disc_phase_t phases[DISC_NUM_PHASES],
                   disc_result_t *result);

/**
 * @brief Classify a single signal from one phase
 *
 * Compares active phase against baseline, runs the appropriate classifier,
 * and returns the best candidate with confidence level.
 * For DISC_SIG_THROTTLE phase, also outputs RPM (rpm_out, rpm_found).
 *
 * @param baseline Baseline phase capture
 * @param active   Active phase capture
 * @param signal   Signal type to classify
 * @param excl     CAN IDs to exclude (already-identified signals)
 * @param excl_count Number of excluded IDs
 * @param out      Output: best candidate
 * @param rpm_out  Output: RPM candidate (only for DISC_SIG_THROTTLE, may be NULL)
 * @param rpm_found Output: whether RPM was found (only for DISC_SIG_THROTTLE, may be NULL)
 * @return true if signal was found
 */
bool disc_classify(const disc_phase_t *baseline,
                   const disc_phase_t *active,
                   disc_signal_t signal,
                   const uint32_t *excl, int excl_count,
                   disc_candidate_t *out,
                   disc_candidate_t *rpm_out, bool *rpm_found);

/**
 * @brief Characterize a detected signal (endianness, signedness, raw range)
 *
 * Analyzes the byte-level statistics from the active phase to determine
 * data type properties. Should be called after disc_classify() succeeds.
 * Requires full-range input during capture for accurate results.
 *
 * @param active Active phase capture (where the signal was detected)
 * @param cand   Candidate to characterize (modified in-place)
 */
void disc_characterize(const disc_phase_t *active, disc_candidate_t *cand);

/**
 * @brief Get human-readable name for a signal type
 */
const char *disc_signal_name(disc_signal_t sig);

/**
 * @brief Get human-readable name for a confidence level
 */
const char *disc_confidence_name(disc_confidence_t conf);

/**
 * @brief Get human-readable name for endianness
 */
const char *disc_endian_name(disc_endian_t e);

/**
 * @brief Get human-readable name for signedness
 */
const char *disc_sign_name(disc_sign_t s);

#ifdef __cplusplus
}
#endif
