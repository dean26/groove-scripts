<?php
/**
 * backfill-ranking-aggregate.php
 *
 * Backfill wp_ranking_aggregate z wp_visit_log dla wskazanego zakresu dat.
 *
 * Liczy tylko:
 * - week (ISO)
 * - month
 * - year
 * - all
 *
 * NIE liczy:
 * - day
 *
 * Jak działa:
 * 1) przyjmuje zakres dat FROM / TO
 * 2) wylicza dotknięte tygodnie / miesiące / lata
 * 3) usuwa z ranking_aggregate tylko agregaty dla tych period_value
 * 4) odbudowuje je z wp_visit_log
 * 5) dla ALL usuwa i przelicza tylko obiekty, które miały wizyty w tym zakresie
 *
 * Uruchom:
 *   wp eval-file backfill-ranking-aggregate.php
 *
 * Opcjonalnie:
 *   GROOVE_FROM="2026-02-01 00:00:00" GROOVE_TO="2026-03-31 23:59:59" wp eval-file backfill-ranking-aggregate.php
 *   GROOVE_DRY_RUN=1 GROOVE_FROM="2026-02-01 00:00:00" GROOVE_TO="2026-03-31 23:59:59" wp eval-file backfill-ranking-aggregate.php
 */

if ( ! defined( 'ABSPATH' ) ) {
    fwrite( STDERR, "Uruchom przez WP-CLI: wp eval-file backfill-ranking-aggregate.php\n" );
    exit( 1 );
}

global $wpdb;
$wpdb->hide_errors();

const GROOVE_BACKFILL_BATCH = 1000;

$from   = getenv( 'GROOVE_FROM' ) ?: '2026-02-01 00:00:00';
$to     = getenv( 'GROOVE_TO' ) ?: '2026-03-31 23:59:59';
$dryRun = false;

$envDry = getenv( 'GROOVE_DRY_RUN' );
if ( $envDry !== false && $envDry !== '' && $envDry !== '0' ) {
    $dryRun = true;
}

function groove_backfill_log( string $msg ): void {
    echo '[' . date( 'Y-m-d H:i:s' ) . '] ' . $msg . PHP_EOL;
}

function groove_backfill_table_exists( string $table ): bool {
    global $wpdb;

    $like  = $wpdb->esc_like( $table );
    $found = $wpdb->get_var( $wpdb->prepare( 'SHOW TABLES LIKE %s', $like ) );

    return ! empty( $found );
}

function groove_backfill_validate_datetime( string $value ): bool {
    $dt = DateTime::createFromFormat( 'Y-m-d H:i:s', $value );
    return $dt && $dt->format( 'Y-m-d H:i:s' ) === $value;
}

/**
 * @return array{weeks:string[],months:string[],years:string[]}
 */
function groove_backfill_collect_periods( string $from, string $to ): array {
    $start = new DateTime( $from );
    $end   = new DateTime( $to );

    $weeks  = [];
    $months = [];
    $years  = [];

    $cursor = clone $start;
    $cursor->setTime( 0, 0, 0 );

    $endDay = clone $end;
    $endDay->setTime( 0, 0, 0 );

    while ( $cursor <= $endDay ) {
        $weeks[ $cursor->format( 'o-\WW' ) ] = true;
        $months[ $cursor->format( 'Y-m' ) ] = true;
        $years[ $cursor->format( 'Y' ) ] = true;
        $cursor->modify( '+1 day' );
    }

    return [
        'weeks'  => array_keys( $weeks ),
        'months' => array_keys( $months ),
        'years'  => array_keys( $years ),
    ];
}

/**
 * @return array<int,array{object_id:int,object_type:string}>
 */
function groove_backfill_get_affected_objects( string $tbl_visits, string $from, string $to ): array {
    global $wpdb;

    $sql = "
		SELECT DISTINCT object_id, object_type
		FROM {$tbl_visits}
		WHERE visited_at >= %s
		  AND visited_at <= %s
		ORDER BY object_type ASC, object_id ASC
	";

    return $wpdb->get_results(
        $wpdb->prepare( $sql, $from, $to ),
        ARRAY_A
    );
}

function groove_backfill_delete_period_values(
    string $tbl_agg,
    string $period_type,
    array $period_values,
    bool $dryRun
): void {
    global $wpdb;

    if ( empty( $period_values ) ) {
        return;
    }

    $chunks = array_chunk( $period_values, 500 );

    foreach ( $chunks as $chunk ) {
        $placeholders = implode( ',', array_fill( 0, count( $chunk ), '%s' ) );

        $sql = "
			DELETE FROM {$tbl_agg}
			WHERE period_type = %s
			  AND period_value IN ({$placeholders})
		";

        $params = array_merge( [ $period_type ], $chunk );

        if ( $dryRun ) {
            groove_backfill_log(
                "[DRY RUN] DELETE {$period_type}: " . count( $chunk ) . " period_value"
            );
            continue;
        }

        $wpdb->query( $wpdb->prepare( $sql, $params ) );

        if ( $wpdb->last_error ) {
            throw new Exception( "DELETE {$period_type} failed: " . $wpdb->last_error );
        }
    }
}

function groove_backfill_delete_all_for_objects(
    string $tbl_agg,
    array $objects,
    bool $dryRun
): void {
    global $wpdb;

    if ( empty( $objects ) ) {
        return;
    }

    $chunks = array_chunk( $objects, 500 );

    foreach ( $chunks as $chunk ) {
        $parts  = [];
        $params = [ 'all', 'all' ];

        foreach ( $chunk as $row ) {
            $parts[]  = '(object_id = %d AND object_type = %s)';
            $params[] = (int) $row['object_id'];
            $params[] = (string) $row['object_type'];
        }

        $sql = "
			DELETE FROM {$tbl_agg}
			WHERE period_type = %s
			  AND period_value = %s
			  AND (" . implode( ' OR ', $parts ) . ")
		";

        if ( $dryRun ) {
            groove_backfill_log(
                "[DRY RUN] DELETE all: " . count( $chunk ) . " objectów"
            );
            continue;
        }

        $wpdb->query( $wpdb->prepare( $sql, $params ) );

        if ( $wpdb->last_error ) {
            throw new Exception( 'DELETE all failed: ' . $wpdb->last_error );
        }
    }
}

function groove_backfill_insert_week(
    string $tbl_visits,
    string $tbl_agg,
    array $weeks,
    bool $dryRun
): void {
    global $wpdb;

    if ( empty( $weeks ) ) {
        return;
    }

    $chunks = array_chunk( $weeks, 200 );

    foreach ( $chunks as $chunk ) {
        $placeholders = implode( ',', array_fill( 0, count( $chunk ), '%s' ) );

        $sql = "
			INSERT INTO {$tbl_agg}
				(object_id, object_type, period_type, period_value, views)
			SELECT
				object_id,
				object_type,
				'week',
				DATE_FORMAT(visited_at, '%x-W%v') AS period_value,
				COUNT(*) AS views
			FROM {$tbl_visits}
			WHERE DATE_FORMAT(visited_at, '%x-W%v') IN ({$placeholders})
			GROUP BY object_id, object_type, DATE_FORMAT(visited_at, '%x-W%v')
		";

        if ( $dryRun ) {
            groove_backfill_log(
                "[DRY RUN] INSERT week: " . count( $chunk ) . " tygodni"
            );
            continue;
        }

        $wpdb->query( $wpdb->prepare( $sql, $chunk ) );

        if ( $wpdb->last_error ) {
            throw new Exception( 'INSERT week failed: ' . $wpdb->last_error );
        }
    }
}

function groove_backfill_insert_month(
    string $tbl_visits,
    string $tbl_agg,
    array $months,
    bool $dryRun
): void {
    global $wpdb;

    if ( empty( $months ) ) {
        return;
    }

    $chunks = array_chunk( $months, 200 );

    foreach ( $chunks as $chunk ) {
        $placeholders = implode( ',', array_fill( 0, count( $chunk ), '%s' ) );

        $sql = "
			INSERT INTO {$tbl_agg}
				(object_id, object_type, period_type, period_value, views)
			SELECT
				object_id,
				object_type,
				'month',
				DATE_FORMAT(visited_at, '%Y-%m') AS period_value,
				COUNT(*) AS views
			FROM {$tbl_visits}
			WHERE DATE_FORMAT(visited_at, '%Y-%m') IN ({$placeholders})
			GROUP BY object_id, object_type, DATE_FORMAT(visited_at, '%Y-%m')
		";

        if ( $dryRun ) {
            groove_backfill_log(
                "[DRY RUN] INSERT month: " . count( $chunk ) . " miesięcy"
            );
            continue;
        }

        $wpdb->query( $wpdb->prepare( $sql, $chunk ) );

        if ( $wpdb->last_error ) {
            throw new Exception( 'INSERT month failed: ' . $wpdb->last_error );
        }
    }
}

function groove_backfill_insert_year(
    string $tbl_visits,
    string $tbl_agg,
    array $years,
    bool $dryRun
): void {
    global $wpdb;

    if ( empty( $years ) ) {
        return;
    }

    $chunks = array_chunk( $years, 200 );

    foreach ( $chunks as $chunk ) {
        $placeholders = implode( ',', array_fill( 0, count( $chunk ), '%s' ) );

        $sql = "
			INSERT INTO {$tbl_agg}
				(object_id, object_type, period_type, period_value, views)
			SELECT
				object_id,
				object_type,
				'year',
				DATE_FORMAT(visited_at, '%Y') AS period_value,
				COUNT(*) AS views
			FROM {$tbl_visits}
			WHERE DATE_FORMAT(visited_at, '%Y') IN ({$placeholders})
			GROUP BY object_id, object_type, DATE_FORMAT(visited_at, '%Y')
		";

        if ( $dryRun ) {
            groove_backfill_log(
                "[DRY RUN] INSERT year: " . count( $chunk ) . " lat"
            );
            continue;
        }

        $wpdb->query( $wpdb->prepare( $sql, $chunk ) );

        if ( $wpdb->last_error ) {
            throw new Exception( 'INSERT year failed: ' . $wpdb->last_error );
        }
    }
}

function groove_backfill_insert_all_for_objects(
    string $tbl_visits,
    string $tbl_agg,
    array $objects,
    bool $dryRun
): void {
    global $wpdb;

    if ( empty( $objects ) ) {
        return;
    }

    $chunks = array_chunk( $objects, 400 );

    foreach ( $chunks as $chunk ) {
        $parts = [];

        foreach ( $chunk as $row ) {
            $parts[] = $wpdb->prepare(
                '(object_id = %d AND object_type = %s)',
                (int) $row['object_id'],
                (string) $row['object_type']
            );
        }

        $whereObjects = implode( ' OR ', $parts );

        $sql = "
			INSERT INTO {$tbl_agg}
				(object_id, object_type, period_type, period_value, views)
			SELECT
				object_id,
				object_type,
				'all',
				'all',
				COUNT(*) AS views
			FROM {$tbl_visits}
			WHERE {$whereObjects}
			GROUP BY object_id, object_type
		";

        if ( $dryRun ) {
            groove_backfill_log(
                "[DRY RUN] INSERT all: " . count( $chunk ) . " objectów"
            );
            continue;
        }

        $wpdb->query( $sql );

        if ( $wpdb->last_error ) {
            throw new Exception( 'INSERT all failed: ' . $wpdb->last_error );
        }
    }
}

/** =============================
 *  START
 *  ============================= */

$t0 = microtime( true );

$tbl_visits = "{$wpdb->prefix}visit_log";
$tbl_agg    = "{$wpdb->prefix}ranking_aggregate";

groove_backfill_log( '=== BACKFILL ranking_aggregate START ===' );
groove_backfill_log( 'FROM=' . $from );
groove_backfill_log( 'TO=' . $to );
groove_backfill_log( 'DRY_RUN=' . ( $dryRun ? 'TRUE' : 'FALSE' ) );

if ( ! groove_backfill_validate_datetime( $from ) ) {
    fwrite( STDERR, "❌ Niepoprawny GROOVE_FROM. Oczekiwany format: Y-m-d H:i:s\n" );
    exit( 1 );
}

if ( ! groove_backfill_validate_datetime( $to ) ) {
    fwrite( STDERR, "❌ Niepoprawny GROOVE_TO. Oczekiwany format: Y-m-d H:i:s\n" );
    exit( 1 );
}

if ( strtotime( $from ) > strtotime( $to ) ) {
    fwrite( STDERR, "❌ GROOVE_FROM nie może być późniejsze niż GROOVE_TO\n" );
    exit( 1 );
}

foreach ( [ $tbl_visits, $tbl_agg ] as $table ) {
    if ( ! groove_backfill_table_exists( $table ) ) {
        fwrite( STDERR, "❌ Brak tabeli: {$table}\n" );
        exit( 1 );
    }
}

$visitsInRange = (int) $wpdb->get_var(
    $wpdb->prepare(
        "SELECT COUNT(*) FROM {$tbl_visits} WHERE visited_at >= %s AND visited_at <= %s",
        $from,
        $to
    )
);

groove_backfill_log( "Visits in range={$visitsInRange}" );

$periods = groove_backfill_collect_periods( $from, $to );
$objects = groove_backfill_get_affected_objects( $tbl_visits, $from, $to );

groove_backfill_log( 'Affected weeks=' . count( $periods['weeks'] ) );
groove_backfill_log( 'Affected months=' . count( $periods['months'] ) );
groove_backfill_log( 'Affected years=' . count( $periods['years'] ) );
groove_backfill_log( 'Affected objects for ALL=' . count( $objects ) );

if ( ! $dryRun ) {
    $wpdb->query( 'START TRANSACTION' );
}

try {
    // 1. usuń dotknięte week/month/year
    groove_backfill_delete_period_values( $tbl_agg, 'week',  $periods['weeks'],  $dryRun );
    groove_backfill_delete_period_values( $tbl_agg, 'month', $periods['months'], $dryRun );
    groove_backfill_delete_period_values( $tbl_agg, 'year',  $periods['years'],  $dryRun );

    // 2. usuń ALL tylko dla obiektów dotkniętych zakresem
    groove_backfill_delete_all_for_objects( $tbl_agg, $objects, $dryRun );

    // 3. odbuduj week/month/year z całego visit_log dla dotkniętych period_value
    groove_backfill_insert_week( $tbl_visits, $tbl_agg, $periods['weeks'], $dryRun );
    groove_backfill_insert_month( $tbl_visits, $tbl_agg, $periods['months'], $dryRun );
    groove_backfill_insert_year( $tbl_visits, $tbl_agg, $periods['years'], $dryRun );

    // 4. odbuduj ALL dla dotkniętych obiektów
    groove_backfill_insert_all_for_objects( $tbl_visits, $tbl_agg, $objects, $dryRun );

    if ( ! $dryRun ) {
        $wpdb->query( 'COMMIT' );
        if ( $wpdb->last_error ) {
            throw new Exception( 'COMMIT failed: ' . $wpdb->last_error );
        }
    }

    $ms = round( ( microtime( true ) - $t0 ) * 1000, 1 );
    groove_backfill_log( "✅ DONE in {$ms} ms" );

} catch ( Throwable $e ) {
    if ( ! $dryRun ) {
        $wpdb->query( 'ROLLBACK' );
    }
    fwrite( STDERR, "❌ BACKFILL ERROR: " . $e->getMessage() . "\n" );
    exit( 1 );
}