<?php
/**
 * sync-relations-since-feb.php
 *
 * Odtwarza relacje tylko dla ŚWIEŻYCH piosenek od START_DATE:
 * - old_artist_song  -> song ↔ artist (+ artist_type)
 * - old_album_song   -> song ↔ album (+ main album)
 * - old_song_genre   -> song ↔ genres
 *
 * Zasada:
 * - bierze tylko piosenki z old_song.{date_column} >= START_DATE
 * - dla każdej z nich czyści relacje w WP
 * - zapisuje dokładnie to, co istnieje w starych tabelach
 *
 * WAŻNE:
 * - nie tworzy brakujących piosenek / artystów / albumów / gatunków
 * - zakłada, że byty zostały już wcześniej dodane i mają old_*_id
 *
 * Uruchom:
 *   wp eval-file sync-fresh-song-relations-since-feb.php
 */

if ( ! defined( 'ABSPATH' ) ) {
	fwrite( STDERR, "Run via WP-CLI: wp eval-file sync-fresh-song-relations-since-feb.php\n" );
	exit( 1 );
}

global $wpdb;

require_once __DIR__ . '/old-db.php';
global $old_db;

/** =============================
 *  KONFIGURACJA
 *  ============================= */

$DRY_RUN = true; // NAJPIERW true
$BATCH   = 300;
$START_DATE = '2026-02-01 00:00:00';

// Legacy tables
$T_OLD_SONG        = 'song';
$T_OLD_ARTIST_SONG = 'artist_song';
$T_OLD_ALBUM_SONG  = 'album_song';
$T_OLD_SONG_GENRE  = 'song_genre';

// WP
$POST_TYPE_SONG = 'song';
$TAX_ARTIST     = 'artist';
$TAX_ALBUM      = 'album';
$TAX_GENRES     = 'genres';

// Meta keys
$META_OLD_SONG_ID   = 'old_song_id';
$META_OLD_ARTIST_ID = 'old_artist_id';
$META_OLD_ALBUM_ID  = 'old_album_id';
$META_OLD_GENRE_ID  = 'old_genre_id';

/** =============================
 *  UTIL
 *  ============================= */

function groove_rel_log( string $msg ): void {
	echo '[' . date( 'Y-m-d H:i:s' ) . '] ' . $msg . PHP_EOL;
}

function groove_rel_table_exists( string $table ): bool {
	global $old_db;
	$like  = $old_db->esc_like( $table );
	$found = $old_db->get_var( $old_db->prepare( 'SHOW TABLES LIKE %s', $like ) );
	return ! empty( $found );
}

function groove_rel_column_exists( string $table, string $column ): bool {
	global $old_db;
	$row = $old_db->get_var(
		$old_db->prepare(
			"SHOW COLUMNS FROM {$table} LIKE %s",
			$column
		)
	);
	return ! empty( $row );
}

function groove_rel_detect_date_column( string $table ): ?string {
	$candidates = [
		'created_date',
		'created_datetime',
		'created_at',
		'inserted_at',
		'date_added',
		'added_date',
		'add_date',
	];

	foreach ( $candidates as $column ) {
		if ( groove_rel_column_exists( $table, $column ) ) {
			return $column;
		}
	}

	return null;
}

/**
 * @return array{rows: array<int,array<string,mixed>>, last_id: int}
 */
function groove_rel_fetch_song_batch_by_date(
	string $table,
	string $date_column,
	string $start_date,
	int $last_id,
	int $limit
): array {
	global $old_db;

	$sql = "
		SELECT id
		FROM {$table}
		WHERE {$date_column} >= %s
		  AND id > %d
		ORDER BY id ASC
		LIMIT %d
	";

	$rows = $old_db->get_results(
		$old_db->prepare( $sql, $start_date, $last_id, $limit ),
		ARRAY_A
	);

	$new_last = $last_id;
	if ( ! empty( $rows ) ) {
		$last = end( $rows );
		$new_last = isset( $last['id'] ) ? (int) $last['id'] : $last_id;
	}

	return [
		'rows'    => $rows,
		'last_id' => $new_last,
	];
}

/**
 * @return array<int,int> old_id => post_id
 */
function groove_rel_map_posts_by_meta( string $meta_key ): array {
	global $wpdb;

	$rows = $wpdb->get_results(
		$wpdb->prepare(
			"SELECT post_id, meta_value FROM {$wpdb->postmeta} WHERE meta_key = %s",
			$meta_key
		),
		ARRAY_A
	);

	$map = [];
	foreach ( $rows as $row ) {
		$map[ (int) $row['meta_value'] ] = (int) $row['post_id'];
	}

	return $map;
}

/**
 * @return array<int,int> old_id => term_id
 */
function groove_rel_map_terms_by_meta( string $meta_key ): array {
	global $wpdb;

	$rows = $wpdb->get_results(
		$wpdb->prepare(
			"SELECT term_id, meta_value FROM {$wpdb->termmeta} WHERE meta_key = %s",
			$meta_key
		),
		ARRAY_A
	);

	$map = [];
	foreach ( $rows as $row ) {
		$map[ (int) $row['meta_value'] ] = (int) $row['term_id'];
	}

	return $map;
}

/**
 * @param int[] $song_ids
 * @return array<int,array<int,int>> old_song_id => [term_id => artist_type]
 */
function groove_rel_fetch_artist_relations( array $song_ids, array $artist_map, string $table ): array {
	global $old_db;

	$song_ids = array_values( array_unique( array_map( 'intval', $song_ids ) ) );
	if ( empty( $song_ids ) ) {
		return [];
	}

	$placeholders = implode( ',', array_fill( 0, count( $song_ids ), '%d' ) );

	$sql = "
		SELECT song_id, artist_id, artist_type
		FROM {$table}
		WHERE song_id IN ({$placeholders})
		ORDER BY song_id ASC
	";

	$rows = $old_db->get_results( $old_db->prepare( $sql, $song_ids ), ARRAY_A );

	$out = [];
	foreach ( $rows as $row ) {
		$old_song_id   = (int) $row['song_id'];
		$old_artist_id = (int) $row['artist_id'];
		$artist_type   = (int) $row['artist_type'];

		if ( ! isset( $artist_map[ $old_artist_id ] ) ) {
			continue;
		}

		$term_id = $artist_map[ $old_artist_id ];

		if ( ! isset( $out[ $old_song_id ] ) ) {
			$out[ $old_song_id ] = [];
		}

		$out[ $old_song_id ][ $term_id ] = $artist_type;
	}

	return $out;
}

/**
 * @param int[] $song_ids
 * @return array<int,array{terms:int[],main_album:?int}>
 */
function groove_rel_fetch_album_relations( array $song_ids, array $album_map, string $table ): array {
	global $old_db;

	$song_ids = array_values( array_unique( array_map( 'intval', $song_ids ) ) );
	if ( empty( $song_ids ) ) {
		return [];
	}

	$placeholders = implode( ',', array_fill( 0, count( $song_ids ), '%d' ) );

	$sql = "
		SELECT song_id, album_id, main
		FROM {$table}
		WHERE song_id IN ({$placeholders})
		ORDER BY song_id ASC
	";

	$rows = $old_db->get_results( $old_db->prepare( $sql, $song_ids ), ARRAY_A );

	$out = [];
	foreach ( $rows as $row ) {
		$old_song_id  = (int) $row['song_id'];
		$old_album_id = (int) $row['album_id'];
		$is_main      = (int) $row['main'] === 1;

		if ( ! isset( $album_map[ $old_album_id ] ) ) {
			continue;
		}

		$term_id = $album_map[ $old_album_id ];

		if ( ! isset( $out[ $old_song_id ] ) ) {
			$out[ $old_song_id ] = [
				'terms'      => [],
				'main_album' => null,
			];
		}

		$out[ $old_song_id ]['terms'][] = $term_id;

		if ( $is_main ) {
			$out[ $old_song_id ]['main_album'] = $term_id;
		}
	}

	foreach ( $out as $song_id => $data ) {
		$out[ $song_id ]['terms'] = array_values( array_unique( array_map( 'intval', $data['terms'] ) ) );
	}

	return $out;
}

/**
 * @param int[] $song_ids
 * @return array<int,int[]>
 */
function groove_rel_fetch_genre_relations( array $song_ids, array $genre_map, string $table ): array {
	global $old_db;

	$song_ids = array_values( array_unique( array_map( 'intval', $song_ids ) ) );
	if ( empty( $song_ids ) ) {
		return [];
	}

	$placeholders = implode( ',', array_fill( 0, count( $song_ids ), '%d' ) );

	$sql = "
		SELECT song_id, genre_id
		FROM {$table}
		WHERE song_id IN ({$placeholders})
		ORDER BY song_id ASC
	";

	$rows = $old_db->get_results( $old_db->prepare( $sql, $song_ids ), ARRAY_A );

	$out = [];
	foreach ( $rows as $row ) {
		$old_song_id  = (int) $row['song_id'];
		$old_genre_id = (int) $row['genre_id'];

		if ( ! isset( $genre_map[ $old_genre_id ] ) ) {
			continue;
		}

		if ( ! isset( $out[ $old_song_id ] ) ) {
			$out[ $old_song_id ] = [];
		}

		$out[ $old_song_id ][] = $genre_map[ $old_genre_id ];
	}

	foreach ( $out as $song_id => $term_ids ) {
		$out[ $song_id ] = array_values( array_unique( array_map( 'intval', $term_ids ) ) );
	}

	return $out;
}

function groove_rel_clear_artist_roles( int $post_id ): void {
	global $wpdb;

	$wpdb->query(
		$wpdb->prepare(
			"DELETE FROM {$wpdb->postmeta}
			 WHERE post_id = %d
			   AND meta_key LIKE 'artist_type_%%'",
			$post_id
		)
	);
}

/** =============================
 *  WALIDACJE
 *  ============================= */

groove_rel_log( '=== SYNC FRESH SONG RELATIONS START ===' );
groove_rel_log( 'DRY_RUN=' . ( $DRY_RUN ? 'true' : 'false' ) );
groove_rel_log( 'START_DATE=' . $START_DATE );
groove_rel_log( 'BATCH=' . $BATCH );

foreach ( [ $T_OLD_SONG, $T_OLD_ARTIST_SONG, $T_OLD_ALBUM_SONG, $T_OLD_SONG_GENRE ] as $table ) {
	if ( ! groove_rel_table_exists( $table ) ) {
		groove_rel_log( "ERROR: table '{$table}' not found." );
		exit( 1 );
	}
}

if ( ! taxonomy_exists( $TAX_ARTIST ) ) {
	groove_rel_log( "ERROR: taxonomy '{$TAX_ARTIST}' does not exist." );
	exit( 1 );
}

if ( ! taxonomy_exists( $TAX_ALBUM ) ) {
	groove_rel_log( "ERROR: taxonomy '{$TAX_ALBUM}' does not exist." );
	exit( 1 );
}

if ( ! taxonomy_exists( $TAX_GENRES ) ) {
	groove_rel_log( "ERROR: taxonomy '{$TAX_GENRES}' does not exist." );
	exit( 1 );
}

if ( ! get_post_type_object( $POST_TYPE_SONG ) ) {
	groove_rel_log( "ERROR: post_type '{$POST_TYPE_SONG}' does not exist." );
	exit( 1 );
}

$song_date_col = groove_rel_detect_date_column( $T_OLD_SONG );
if ( ! $song_date_col ) {
	groove_rel_log( "ERROR: table '{$T_OLD_SONG}' does not have a recognized created_* date column." );
	exit( 1 );
}

groove_rel_log( 'Detected song date column: ' . $song_date_col );

/** =============================
 *  MAPY
 *  ============================= */

groove_rel_log( 'Loading maps…' );

$song_map   = groove_rel_map_posts_by_meta( $META_OLD_SONG_ID );
$artist_map = groove_rel_map_terms_by_meta( $META_OLD_ARTIST_ID );
$album_map  = groove_rel_map_terms_by_meta( $META_OLD_ALBUM_ID );
$genre_map  = groove_rel_map_terms_by_meta( $META_OLD_GENRE_ID );

groove_rel_log(
	'Maps loaded: songs=' . count( $song_map ) .
	', artists=' . count( $artist_map ) .
	', albums=' . count( $album_map ) .
	', genres=' . count( $genre_map )
);

/** =============================
 *  SYNC
 *  ============================= */

$last_id   = 0;
$processed = 0;
$updated   = 0;
$skipped_missing_song = 0;

while ( true ) {
	$pack = groove_rel_fetch_song_batch_by_date(
		$T_OLD_SONG,
		$song_date_col,
		$START_DATE,
		$last_id,
		$BATCH
	);

	$rows    = $pack['rows'];
	$last_id = $pack['last_id'];

	if ( empty( $rows ) ) {
		break;
	}

	$old_song_ids = [];
	foreach ( $rows as $row ) {
		$old_song_ids[] = (int) $row['id'];
	}

	$artist_rel = groove_rel_fetch_artist_relations( $old_song_ids, $artist_map, $T_OLD_ARTIST_SONG );
	$album_rel  = groove_rel_fetch_album_relations( $old_song_ids, $album_map, $T_OLD_ALBUM_SONG );
	$genre_rel  = groove_rel_fetch_genre_relations( $old_song_ids, $genre_map, $T_OLD_SONG_GENRE );

	foreach ( $old_song_ids as $old_song_id ) {
		$processed++;

		if ( ! isset( $song_map[ $old_song_id ] ) ) {
			$skipped_missing_song++;
			continue;
		}

		$post_id = $song_map[ $old_song_id ];

		$artist_terms = $artist_rel[ $old_song_id ] ?? [];
		$album_data   = $album_rel[ $old_song_id ] ?? [ 'terms' => [], 'main_album' => null ];
		$genre_terms  = $genre_rel[ $old_song_id ] ?? [];

		if ( $DRY_RUN ) {
			if ( $processed % 200 === 0 ) {
				groove_rel_log(
					"WOULD UPDATE old_song_id={$old_song_id} post_id={$post_id} artists=" . count( $artist_terms ) .
					" albums=" . count( $album_data['terms'] ) .
					" genres=" . count( $genre_terms )
				);
			}
			$updated++;
			continue;
		}

		// ===== ARTISTS =====
		wp_set_object_terms( $post_id, array_keys( $artist_terms ), $TAX_ARTIST, false );
		groove_rel_clear_artist_roles( $post_id );

		foreach ( $artist_terms as $term_id => $artist_type ) {
			update_post_meta( $post_id, 'artist_type_' . $term_id, $artist_type );
		}

		// ===== ALBUMS =====
		wp_set_object_terms( $post_id, $album_data['terms'], $TAX_ALBUM, false );

		if ( ! empty( $album_data['main_album'] ) ) {
			update_post_meta( $post_id, 'main_album', (int) $album_data['main_album'] );
		} else {
			delete_post_meta( $post_id, 'main_album' );
		}

		// ===== GENRES =====
		wp_set_object_terms( $post_id, $genre_terms, $TAX_GENRES, false );

		$updated++;

		if ( $processed % 500 === 0 ) {
			groove_rel_log(
				"Processed={$processed}, updated={$updated}, skipped_missing_song={$skipped_missing_song}"
			);
		}
	}
}

groove_rel_log( '=== DONE ===' );
groove_rel_log(
	'Summary: processed=' . $processed .
	', updated=' . $updated .
	', skipped_missing_song=' . $skipped_missing_song
);

if ( $DRY_RUN ) {
	groove_rel_log( 'DRY_RUN=true -> no changes written.' );
}