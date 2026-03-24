<?php
/**
 * sync-song-last-view.php
 *
 * Kopiuje dane z tabeli Song_last_view (stara baza) do wp_ranking_aggregate (WP).
 *
 * Song_last_view:       id, song_id, views, created_date
 * wp_ranking_aggregate: id, object_id, object_type, period_type, period_value, views, year_int
 *
 * Mapowanie:
 *   object_id    = song_id
 *   object_type  = 'song'
 *   period_type  = 'day'
 *   period_value = created_date (np. '2020-12-31')
 *   views        = views
 *   year_int     = YEAR(created_date)
 *
 * Uruchom:
 *   wp eval-file sync-song-last-view.php
 *
 * Opcjonalnie:
 *   GROOVE_DRY_RUN=1 wp eval-file sync-song-last-view.php
 *   GROOVE_FROM="2020-01-01" wp eval-file sync-song-last-view.php
 *   GROOVE_FROM="2020-01-01" GROOVE_DRY_RUN=1 wp eval-file sync-song-last-view.php
 */

if ( ! defined( 'ABSPATH' ) ) {
	fwrite( STDERR, "Uruchom przez WP-CLI: wp eval-file sync-song-last-view.php\n" );
	exit( 1 );
}

global $wpdb;

require_once __DIR__ . '/old-db.php';
global $old_db;

/** =============================
 *  KONFIGURACJA
 *  ============================= */

$DRY_RUN = false;
$BATCH   = 1000;

$envDry = getenv( 'GROOVE_DRY_RUN' );
if ( $envDry !== false && $envDry !== '' && $envDry !== '0' ) {
	$DRY_RUN = true;
}

$FROM = getenv( 'GROOVE_FROM' ) ?: '2026-02-01';

// Tabele
$T_OLD_SONG_LAST_VIEW = 'Song_last_view';
$T_AGG                = "{$wpdb->prefix}ranking_aggregate";

/** =============================
 *  UTIL
 *  ============================= */

function slv_log( string $msg ): void {
	echo '[' . date( 'Y-m-d H:i:s' ) . '] ' . $msg . PHP_EOL;
}

function slv_table_exists_old( string $table ): bool {
	global $old_db;

	$like  = $old_db->esc_like( $table );
	$found = $old_db->get_var( $old_db->prepare( 'SHOW TABLES LIKE %s', $like ) );

	return ! empty( $found );
}

function slv_table_exists_wp( string $table ): bool {
	global $wpdb;

	$like  = $wpdb->esc_like( $table );
	$found = $wpdb->get_var( $wpdb->prepare( 'SHOW TABLES LIKE %s', $like ) );

	return ! empty( $found );
}

/** =============================
 *  START
 *  ============================= */

$t0 = microtime( true );

slv_log( '=== sync-song-last-view START ===' );
slv_log( 'DRY_RUN=' . ( $DRY_RUN ? 'TRUE' : 'FALSE' ) );
slv_log( 'FROM=' . $FROM );
slv_log( 'BATCH=' . $BATCH );

// Walidacja tabel
if ( ! slv_table_exists_old( $T_OLD_SONG_LAST_VIEW ) ) {
	fwrite( STDERR, "Brak tabeli: {$T_OLD_SONG_LAST_VIEW} (stara baza)\n" );
	exit( 1 );
}

if ( ! slv_table_exists_wp( $T_AGG ) ) {
	fwrite( STDERR, "Brak tabeli: {$T_AGG} (WP)\n" );
	exit( 1 );
}

// Ile rekordow do przeniesienia
$totalRows = (int) $old_db->get_var(
	$old_db->prepare(
		"SELECT COUNT(*) FROM {$T_OLD_SONG_LAST_VIEW} WHERE created_date >= %s",
		$FROM
	)
);

slv_log( "Rows to migrate: {$totalRows}" );

if ( $totalRows === 0 ) {
	slv_log( 'Nothing to do.' );
	exit( 0 );
}

/** =============================
 *  MIGRACJA
 *  ============================= */

$lastId    = 0;
$inserted  = 0;
$skipped   = 0;
$errors    = 0;
$batchNum  = 0;

while ( true ) {
	$rows = $old_db->get_results(
		$old_db->prepare(
			"SELECT id, song_id, views, created_date
			 FROM {$T_OLD_SONG_LAST_VIEW}
			 WHERE created_date >= %s
			   AND id > %d
			 ORDER BY id ASC
			 LIMIT %d",
			$FROM,
			$lastId,
			$BATCH
		),
		ARRAY_A
	);

	if ( empty( $rows ) ) {
		break;
	}

	$batchNum++;
	$lastId = (int) end( $rows )['id'];

	if ( $DRY_RUN ) {
		$inserted += count( $rows );
		slv_log(
			"[DRY RUN] Batch #{$batchNum}: " . count( $rows ) . " rows (last_id={$lastId}), "
			. "sample: song_id=" . $rows[0]['song_id'] . " date=" . $rows[0]['created_date'] . " views=" . $rows[0]['views']
		);
		continue;
	}

	// Budujemy INSERT batch
	$values  = [];
	$params  = [];

	foreach ( $rows as $row ) {
		$songId     = (int) $row['song_id'];
		$views      = (int) $row['views'];
		$date       = $row['created_date'];
		$yearInt    = (int) substr( $date, 0, 4 );

		if ( $songId <= 0 || $views <= 0 ) {
			$skipped++;
			continue;
		}

		$values[] = '(%d, %s, %s, %s, %d, %d)';
		$params[] = $songId;
		$params[] = 'song';
		$params[] = 'day';
		$params[] = $date;
		$params[] = $views;
		$params[] = $yearInt;
	}

	if ( empty( $values ) ) {
		continue;
	}

	$sql = "INSERT INTO {$T_AGG} (object_id, object_type, period_type, period_value, views, year_int) VALUES "
		 . implode( ', ', $values );

	$wpdb->query( $wpdb->prepare( $sql, $params ) );

	if ( $wpdb->last_error ) {
		slv_log( "ERROR batch #{$batchNum}: " . $wpdb->last_error );
		$errors++;
	} else {
		$inserted += count( $values );
	}

	if ( $batchNum % 10 === 0 ) {
		slv_log( "Progress: batch #{$batchNum}, inserted={$inserted}, skipped={$skipped}, errors={$errors}" );
	}
}

/** =============================
 *  SUMMARY
 *  ============================= */

$ms = round( ( microtime( true ) - $t0 ) * 1000, 1 );

slv_log( '=== SUMMARY ===' );
slv_log( "Total source rows: {$totalRows}" );
slv_log( "Inserted: {$inserted}" );
slv_log( "Skipped: {$skipped}" );
slv_log( "Errors: {$errors}" );
slv_log( "Done in {$ms} ms" );

if ( $DRY_RUN ) {
	slv_log( 'DRY_RUN=TRUE -> no data was written. Run without GROOVE_DRY_RUN to execute.' );
}
