<?php
/**
 * sync-map-existing-artists.php
 *
 * Mapuje ISTNIEJĄCYCH artystów w WP do old_artists:
 * - match po nazwie (old_artists.name === wp_terms.name)
 * - jeśli term NIE ma meta old_artist_id → dodaje je
 *
 * NIE tworzy nowych terminów
 * NIE nadpisuje istniejącego old_artist_id
 *
 * Uruchom:
 *   wp eval-file map-existing-artists.php
 */

if ( ! defined( 'ABSPATH' ) ) {
    $guess = dirname( __FILE__ );
    for ( $i = 0; $i < 6; $i++ ) {
        if ( file_exists( $guess . '/wp-load.php' ) ) {
            require_once $guess . '/wp-load.php';
            break;
        }
        $guess = dirname( $guess );
    }
}

global $wpdb;

require_once __DIR__ . '/old-db.php';
global $old_db;

/** =============================
 *  KONFIGURACJA
 *  ============================= */

$DRY_RUN = false; // ← true = test, false = zapis
$TAX_ARTIST = 'artist';
$T_OLD_ARTISTS = 'artist';
$META_KEY = 'old_artist_id';

/** =============================
 *  UTIL
 *  ============================= */

function log_line( string $msg ): void {
    echo '[' . date('Y-m-d H:i:s') . '] ' . $msg . PHP_EOL;
}

/** =============================
 *  START
 *  ============================= */

log_line('=== map-existing-artists START ===');
log_line('DRY_RUN: ' . ( $DRY_RUN ? 'true (no writes)' : 'false (WILL WRITE)' ));

/**
 * 1) Pobieramy wszystkich artystów z bazy produkcyjnej
 * 2) Dopasowujemy po nazwie do wp_terms w bazie WP
 */
$old_artists = $old_db->get_results(
    "SELECT id, name FROM {$T_OLD_ARTISTS} ORDER BY id ASC",
    ARRAY_A
);

// Budujemy mapę: name => [old_id, ...]
$old_by_name = [];
foreach ( $old_artists as $oa ) {
    $old_by_name[ $oa['name'] ][] = (int) $oa['id'];
}

// Pobieramy dopasowane termy z WP
$names_escaped = array_map( function( $n ) use ( $wpdb ) {
    return $wpdb->prepare( '%s', $n );
}, array_keys( $old_by_name ) );

$rows = [];
if ( ! empty( $names_escaped ) ) {
    $in_clause = implode( ',', $names_escaped );
    $sql = "
    SELECT
        t.term_id,
        t.name AS name,
        tm.meta_value AS existing_old_id
    FROM {$wpdb->terms} t
    LEFT JOIN {$wpdb->termmeta} tm
        ON tm.term_id = t.term_id
       AND tm.meta_key = %s
    WHERE t.name IN ({$in_clause})
    ORDER BY t.term_id ASC
    ";

    $wp_rows = $wpdb->get_results(
        $wpdb->prepare( $sql, $META_KEY ),
        ARRAY_A
    );

    // Łączymy: dla każdego WP termu szukamy old_id po nazwie
    foreach ( $wp_rows as $wr ) {
        $name = $wr['name'];
        if ( isset( $old_by_name[ $name ] ) ) {
            foreach ( $old_by_name[ $name ] as $old_id ) {
                $rows[] = [
                    'old_id'          => $old_id,
                    'name'            => $name,
                    'term_id'         => $wr['term_id'],
                    'existing_old_id' => $wr['existing_old_id'],
                ];
            }
        }
    }
}

$checked = 0;
$updated = 0;
$skipped = 0;

foreach ( $rows as $r ) {
    $checked++;

    $old_id   = (int) $r['old_id'];
    $term_id  = (int) $r['term_id'];
    $existing = $r['existing_old_id'];

    if ( $existing !== null && $existing !== '' ) {
        $skipped++;
        continue;
    }

    if ( $DRY_RUN ) {
        log_line("WOULD MAP term_id={$term_id} ← old_artist_id={$old_id} ({$r['name']})");
        $updated++;
        continue;
    }

    update_term_meta( $term_id, $META_KEY, $old_id );
    $updated++;

    if ( $updated % 500 === 0 ) {
        log_line("progress updated={$updated}");
    }
}

log_line('=== SUMMARY ===');
log_line("checked={$checked}");
log_line("mapped={$updated}");
log_line("skipped_existing={$skipped}");
log_line('=== DONE ===');