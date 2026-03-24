<?php

/**
 * force-rebuild-ranking-stats.php
 *
 * Wymusza rebuild wskazanych okresów w wp_ranking_stats
 * na podstawie wp_ranking_aggregate.
 *
 * Obsługiwane okresy:
 * - week
 * - month
 * - year
 * - all
 *
 * Przykład użycia:
 *
 * 1) tylko miesiące luty + marzec 2026 i rok 2026 + all
 * GROOVE_FORCE_MONTHS="2026-02,2026-03" \
 * GROOVE_FORCE_YEARS="2026" \
 * GROOVE_FORCE_ALL=1 \
 * wp eval-file force-rebuild-ranking-stats.php
 *
 * 2) tylko tygodnie
 * GROOVE_FORCE_WEEKS="2026-W05,2026-W06,2026-W07" \
 * wp eval-file force-rebuild-ranking-stats.php
 *
 * 3) dry run
 * GROOVE_DRY_RUN=1 \
 * GROOVE_FORCE_MONTHS="2026-02,2026-03" \
 * GROOVE_FORCE_YEARS="2026" \
 * GROOVE_FORCE_ALL=1 \
 * wp eval-file force-rebuild-ranking-stats.php
 */

if (!defined('ABSPATH')) {
    fwrite(STDERR, "Uruchom przez WP-CLI: wp eval-file force-rebuild-ranking-stats.php\n");
    exit(1);
}

global $wpdb;
$wpdb->hide_errors();

const GROOVE_FORCE_TOP_LIMIT = 100;

$dryRun = false;
$envDry = getenv('GROOVE_DRY_RUN');
if ($envDry !== false && $envDry !== '' && $envDry !== '0') {
    $dryRun = true;
}

$forceWeeks  = groove_force_parse_csv_env('GROOVE_FORCE_WEEKS');
$forceMonths = groove_force_parse_csv_env('GROOVE_FORCE_MONTHS');
$forceYears  = groove_force_parse_csv_env('GROOVE_FORCE_YEARS');
$forceAll    = getenv('GROOVE_FORCE_ALL');

function groove_force_log(string $msg): void
{
    echo '[' . date('Y-m-d H:i:s') . '] ' . $msg . PHP_EOL;
}

function groove_force_parse_csv_env(string $key): array
{
    $val = getenv($key);
    if ($val === false || trim($val) === '') {
        return [];
    }

    $items = array_map('trim', explode(',', $val));
    $items = array_values(array_filter(array_unique($items), static fn($v) => $v !== ''));

    return $items;
}

function groove_force_table_exists(string $table): bool
{
    global $wpdb;

    $like  = $wpdb->esc_like($table);
    $found = $wpdb->get_var($wpdb->prepare('SHOW TABLES LIKE %s', $like));

    return !empty($found);
}

function groove_force_validate_week(string $value): bool
{
    return (bool) preg_match('/^\d{4}-W\d{2}$/', $value);
}

function groove_force_validate_month(string $value): bool
{
    return (bool) preg_match('/^\d{4}-\d{2}$/', $value);
}

function groove_force_validate_year(string $value): bool
{
    return (bool) preg_match('/^\d{4}$/', $value);
}

/**
 * @param int[] $objectIds
 * @return array<int,array{position_current:int,position_peak:int,streak:int}>
 */
function groove_force_get_previous_map(
    string $table_stats,
    string $object_type,
    string $period_type,
    string $period_value,
    array $objectIds
): array {
    global $wpdb;

    if (empty($objectIds)) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($objectIds), '%d'));

    $sql = $wpdb->prepare(
        "
        SELECT object_id, position_current, position_peak, streak
        FROM {$table_stats}
        WHERE object_type = %s
          AND period_type = %s
          AND period_value = %s
          AND object_id IN ({$placeholders})
        ",
        array_merge([$object_type, $period_type, $period_value], $objectIds)
    );

    $rows = $wpdb->get_results($sql, ARRAY_A) ?: [];

    $map = [];
    foreach ($rows as $row) {
        $map[(int)$row['object_id']] = [
            'position_current' => (int)$row['position_current'],
            'position_peak'    => (int)$row['position_peak'],
            'streak'           => (int)$row['streak'],
        ];
    }

    return $map;
}

/**
 * @param int[] $objectIds
 * @return array<int,int>
 */
function groove_force_get_history_peak_map(
    string $table_stats,
    string $object_type,
    string $period_type,
    string $period_value,
    array $objectIds
): array {
    global $wpdb;

    if (empty($objectIds)) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($objectIds), '%d'));

    $sql = $wpdb->prepare(
        "
        SELECT object_id, MIN(position_peak) AS min_peak
        FROM {$table_stats}
        WHERE object_type = %s
          AND period_type = %s
          AND period_value < %s
          AND object_id IN ({$placeholders})
        GROUP BY object_id
        ",
        array_merge([$object_type, $period_type, $period_value], $objectIds)
    );

    $rows = $wpdb->get_results($sql, ARRAY_A) ?: [];

    $map = [];
    foreach ($rows as $row) {
        $map[(int)$row['object_id']] = (int)$row['min_peak'];
    }

    return $map;
}

/**
 * @param int[] $objectIds
 * @return array<int,int>
 */
function groove_force_get_existing_peak_map_for_all(
    string $table_stats,
    string $object_type,
    array $objectIds
): array {
    global $wpdb;

    if (empty($objectIds)) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($objectIds), '%d'));

    $sql = $wpdb->prepare(
        "
        SELECT object_id, position_peak
        FROM {$table_stats}
        WHERE object_type = %s
          AND period_type = 'all'
          AND period_value = 'all'
          AND object_id IN ({$placeholders})
        ",
        array_merge([$object_type], $objectIds)
    );

    $rows = $wpdb->get_results($sql, ARRAY_A) ?: [];

    $map = [];
    foreach ($rows as $row) {
        $map[(int)$row['object_id']] = (int)$row['position_peak'];
    }

    return $map;
}

/**
 * @return array{0:string,1:string}|null
 */
function groove_force_get_previous_period(string $period_type, string $period_value): ?array
{
    switch ($period_type) {
        case 'week':
            if (!preg_match('/^(\d{4})-W(\d{2})$/', $period_value, $m)) {
                return null;
            }

            $year = (int) $m[1];
            $week = (int) $m[2];

            $dt = new DateTime('now', new DateTimeZone('UTC'));
            $dt->setISODate($year, $week, 1);
            $dt->modify('-7 day');

            return ['week', sprintf('%s-W%02d', $dt->format('o'), (int) $dt->format('W'))];

        case 'month':
            $dt = DateTime::createFromFormat('Y-m-d', $period_value . '-01', new DateTimeZone('UTC'));
            if (!$dt) {
                return null;
            }
            $dt->modify('-1 month');
            return ['month', $dt->format('Y-m')];

        case 'year':
            $year = (int) $period_value;
            if ($year <= 0) {
                return null;
            }
            return ['year', (string) ($year - 1)];

        default:
            return null;
    }
}

function groove_force_build_for_period(
    string $table_agg,
    string $table_stats,
    string $object_type,
    string $period_type,
    string $period_value,
    bool $dryRun
): void {
    global $wpdb;

    $is_all = ($period_type === 'all');
    if ($is_all) {
        $period_value = 'all';
    }

    $sqlTop = $wpdb->prepare(
        "
        SELECT object_id, views
        FROM {$table_agg}
        WHERE object_type = %s
          AND period_type = %s
          AND period_value = %s
        ORDER BY views DESC, object_id ASC
        LIMIT %d
        ",
        $object_type,
        $period_type,
        $period_value,
        GROOVE_FORCE_TOP_LIMIT
    );

    $topRows = $wpdb->get_results($sqlTop, ARRAY_A) ?: [];

    if (empty($topRows)) {
        groove_force_log(sprintf(
            'ℹ️ Brak TOP dla %s / %s / %s',
            $object_type,
            $period_type,
            $period_value
        ));
        return;
    }

    $objectIds = array_map(static fn($row): int => (int) $row['object_id'], $topRows);

    $prevMap = [];
    if (!$is_all) {
        $prevPeriod = groove_force_get_previous_period($period_type, $period_value);
        if ($prevPeriod !== null) {
            [$prevType, $prevValue] = $prevPeriod;
            $prevMap = groove_force_get_previous_map(
                $table_stats,
                $object_type,
                $prevType,
                $prevValue,
                $objectIds
            );
        }
    }

    $historyPeakMap = $is_all
        ? groove_force_get_existing_peak_map_for_all($table_stats, $object_type, $objectIds)
        : groove_force_get_history_peak_map($table_stats, $object_type, $period_type, $period_value, $objectIds);

    $values   = [];
    $params   = [];
    $position = 0;

    foreach ($topRows as $row) {
        $position++;
        $object_id = (int) $row['object_id'];
        $views     = (int) $row['views'];

        if ($is_all) {
            $position_previous = 0;
            $position_peak     = isset($historyPeakMap[$object_id])
                ? min((int) $historyPeakMap[$object_id], $position)
                : $position;
            $streak    = 1;
            $is_return = 0;
        } else {
            $prev       = $prevMap[$object_id] ?? null;
            $hadHistory = array_key_exists($object_id, $historyPeakMap);

            $position_previous = $prev['position_current'] ?? 0;
            $position_peak     = $hadHistory
                ? min((int) $historyPeakMap[$object_id], $position)
                : $position;
            $streak    = $prev ? ((int) $prev['streak'] + 1) : 1;
            $is_return = ($hadHistory && !$prev) ? 1 : 0;
        }

        $values[] = "(%d, %s, %s, %s, %d, %d, %d, %d, %d, %d, NOW(), NOW())";

        $params[] = $object_id;
        $params[] = $object_type;
        $params[] = $period_type;
        $params[] = $period_value;
        $params[] = $position;
        $params[] = $position_previous;
        $params[] = $position_peak;
        $params[] = $streak;
        $params[] = $is_return;
        $params[] = $views;
    }

    if ($dryRun) {
        groove_force_log(sprintf(
            '[DRY RUN] %s / %s / %s -> %d rekordów',
            $object_type,
            $period_type,
            $period_value,
            count($topRows)
        ));
        return;
    }

    $sqlInsert = "
        INSERT INTO {$table_stats}
            (
                object_id,
                object_type,
                period_type,
                period_value,
                position_current,
                position_previous,
                position_peak,
                streak,
                is_return,
                views,
                created_at,
                updated_at
            )
        VALUES " . implode(',', $values) . "
        ON DUPLICATE KEY UPDATE
            views             = VALUES(views),
            position_current  = VALUES(position_current),
            position_previous = VALUES(position_previous),
            position_peak     = VALUES(position_peak),
            streak            = VALUES(streak),
            is_return         = VALUES(is_return),
            updated_at        = NOW()
    ";

    $prepared = $wpdb->prepare($sqlInsert, $params);
    $wpdb->query($prepared);

    if ($wpdb->last_error) {
        throw new Exception(sprintf(
            'SQL error dla %s/%s/%s: %s',
            $object_type,
            $period_type,
            $period_value,
            $wpdb->last_error
        ));
    }

    groove_force_log(sprintf(
        '✅ Zapisano %d rekordów dla %s / %s / %s',
        count($topRows),
        $object_type,
        $period_type,
        $period_value
    ));
}

/** =============================
 * START
 * ============================= */

$tableAgg   = $wpdb->prefix . 'ranking_aggregate';
$tableStats = $wpdb->prefix . 'ranking_stats';

groove_force_log('=== FORCE REBUILD ranking_stats START ===');
groove_force_log('DRY_RUN=' . ($dryRun ? 'TRUE' : 'FALSE'));
groove_force_log('WEEKS=' . implode(',', $forceWeeks));
groove_force_log('MONTHS=' . implode(',', $forceMonths));
groove_force_log('YEARS=' . implode(',', $forceYears));
groove_force_log('ALL=' . (($forceAll !== false && $forceAll !== '' && $forceAll !== '0') ? 'YES' : 'NO'));

foreach ([$tableAgg, $tableStats] as $table) {
    if (!groove_force_table_exists($table)) {
        fwrite(STDERR, "❌ Brak tabeli: {$table}\n");
        exit(1);
    }
}

foreach ($forceWeeks as $week) {
    if (!groove_force_validate_week($week)) {
        fwrite(STDERR, "❌ Niepoprawny tydzień: {$week}\n");
        exit(1);
    }
}

foreach ($forceMonths as $month) {
    if (!groove_force_validate_month($month)) {
        fwrite(STDERR, "❌ Niepoprawny miesiąc: {$month}\n");
        exit(1);
    }
}

foreach ($forceYears as $year) {
    if (!groove_force_validate_year($year)) {
        fwrite(STDERR, "❌ Niepoprawny rok: {$year}\n");
        exit(1);
    }
}

$periods = [];

foreach ($forceWeeks as $week) {
    $periods[] = ['period_type' => 'week', 'period_value' => $week];
}
foreach ($forceMonths as $month) {
    $periods[] = ['period_type' => 'month', 'period_value' => $month];
}
foreach ($forceYears as $year) {
    $periods[] = ['period_type' => 'year', 'period_value' => $year];
}

if (empty($periods) && !($forceAll !== false && $forceAll !== '' && $forceAll !== '0')) {
    fwrite(STDERR, "❌ Nie podano żadnych okresów do rebuildu.\n");
    exit(1);
}

try {
    foreach ($periods as $period) {
        foreach (['song', 'album', 'artist'] as $objectType) {
            groove_force_build_for_period(
                $tableAgg,
                $tableStats,
                $objectType,
                $period['period_type'],
                $period['period_value'],
                $dryRun
            );
        }
    }

    if ($forceAll !== false && $forceAll !== '' && $forceAll !== '0') {
        foreach (['song', 'album', 'artist'] as $objectType) {
            groove_force_build_for_period(
                $tableAgg,
                $tableStats,
                $objectType,
                'all',
                'all',
                $dryRun
            );
        }
    }

    groove_force_log('=== DONE ===');

} catch (Throwable $e) {
    fwrite(STDERR, "❌ ERROR: " . $e->getMessage() . "\n");
    exit(1);
}