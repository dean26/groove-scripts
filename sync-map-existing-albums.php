<?php
/**
 * sync-map-existing-albums.php
 *
 * Mapuje ISTNIEJĄCE albumy w WP do old_album:
 * - match po title (old_album.title === wp_terms.name)
 * - jeśli term NIE ma meta old_album_id → dodaje je
 *
 * NIE tworzy nowych terminów
 * NIE nadpisuje istniejącego old_album_id
 *
 * Uruchom:
 *   wp eval-file sync-map-existing-albums.php
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

$DRY_RUN = false; // ← NAJPIERW TRUE
$TAX = 'album';
$T_OLD_ALBUM = 'Album';
$META_KEY = 'old_album_id';

/** =============================
 *  UTIL
 *  ============================= */

function log_line( string $msg ): void {
    echo '[' . date('Y-m-d H:i:s') . '] ' . $msg . PHP_EOL;
}

/** =============================
 *  START
 *  ============================= */

log_line('=== sync-map-existing-albums START ===');
log_line('DRY_RUN: ' . ( $DRY_RUN ? 'true (no writes)' : 'false (WILL WRITE)' ));

/**
 * 1) Pobieramy wszystkie old_album z bazy produkcyjnej
 * 2) Dopasowujemy po tytule do wp_terms w bazie WP
 */
$old_albums = $old_db->get_results(
    "SELECT id, title FROM {$T_OLD_ALBUM} ORDER BY id ASC",
    ARRAY_A
);

// Budujemy mapę: title => [old_id, ...]
$old_by_title = [];
foreach ( $old_albums as $oa ) {
    $old_by_title[ $oa['title'] ][] = (int) $oa['id'];
}

// Pobieramy dopasowane termy z WP
$titles_escaped = array_map( function( $t ) use ( $wpdb ) {
    return $wpdb->prepare( '%s', $t );
}, array_keys( $old_by_title ) );

$rows = [];
if ( ! empty( $titles_escaped ) ) {
    $in_clause = implode( ',', $titles_escaped );
    $sql = "
    SELECT
        t.term_id,
        t.name AS title,
        tm.meta_value AS existing_old_id
    FROM {$wpdb->terms} t
    INNER JOIN {$wpdb->term_taxonomy} tt
        ON tt.term_id = t.term_id
       AND tt.taxonomy = %s
    LEFT JOIN {$wpdb->termmeta} tm
        ON tm.term_id = t.term_id
       AND tm.meta_key = %s
    WHERE t.name IN ({$in_clause})
    ORDER BY t.term_id ASC
    ";

    $wp_rows = $wpdb->get_results(
        $wpdb->prepare( $sql, $TAX, $META_KEY ),
        ARRAY_A
    );

    // Łączymy: dla każdego WP termu szukamy old_id po tytule
    foreach ( $wp_rows as $wr ) {
        $title = $wr['title'];
        if ( isset( $old_by_title[ $title ] ) ) {
            foreach ( $old_by_title[ $title ] as $old_id ) {
                $rows[] = [
                    'old_id'          => $old_id,
                    'title'           => $title,
                    'term_id'         => $wr['term_id'],
                    'existing_old_id' => $wr['existing_old_id'],
                ];
            }
        }
    }
}

$checked = 0;
$mapped  = 0;
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
        log_line("WOULD MAP term_id={$term_id} ← old_album_id={$old_id} ({$r['title']})");
        $mapped++;
        continue;
    }

    update_term_meta( $term_id, $META_KEY, $old_id );
    $mapped++;

    if ( $mapped % 500 === 0 ) {
        log_line("progress mapped={$mapped}");
    }
}

/** =============================
 *  SUMMARY
 *  ============================= */

log_line('=== SUMMARY ===');
log_line("checked={$checked}");
log_line("mapped={$mapped}");
log_line("skipped_existing={$skipped}");
log_line('=== DONE ===');

if ( $DRY_RUN ) {
    log_line('DRY_RUN enabled → no changes written');
}