<?php
/**
 * sync-fresh-entities-since-feb.php
 *
 * Dodaje tylko ŚWIEŻE brakujące byty od wskazanej daty:
 * - old_song               -> post_type 'song'
 * - old_artists/old_artist -> taxonomy 'artist'
 * - old_album              -> taxonomy 'album'
 *
 * Zasada działania:
 * 1) bierze piosenki z old_song.{date_column} >= START_DATE
 * 2) dodaje brakujące piosenki
 * 3) znajduje artystów i albumy powiązane z tymi piosenkami
 * 4) dodaje brakujących artystów i albumy
 * 5) dodatkowo skanuje old_artists / old_album po ID (od START_ARTIST_ID / START_ALBUM_ID)
 *    i dogrywa brakujące byty
 *
 * WAŻNE:
 * - nie dotyka relacji song <-> artist / album / genres
 * - nie dotyka odwiedzin
 * - nie aktualizuje istniejących postów
 * - nie używa handle_url do slugów
 *
 * Uruchom:
 *   wp eval-file sync-fresh-entities-since-feb.php
 */

if ( ! defined( 'ABSPATH' ) ) {
	$guess = dirname( __FILE__ );
	$found = false;

	for ( $i = 0; $i < 6; $i++ ) {
		$candidate = $guess . '/wp-load.php';
		if ( file_exists( $candidate ) ) {
			require_once $candidate;
			$found = true;
			break;
		}
		$guess = dirname( $guess );
	}

	if ( ! $found ) {
		fwrite( STDERR, "ERROR: ABSPATH not defined and wp-load.php not found. Run via WP-CLI: wp eval-file sync-fresh-entities-since-feb.php\n" );
		exit( 1 );
	}
}

global $wpdb;

require_once __DIR__ . '/old-db.php';
global $old_db;

/** =============================
 *  KONFIGURACJA
 *  ============================= */

$DRY_RUN = true; // NAJPIERW true
$BATCH   = 300;

/**
 * Dla obecnej sytuacji "od lutego do teraz" ustawiam 2026-02-01.
 * Jeśli chcesz jednak od lutego 2025, zmień poniżej.
 */
$START_DATE = '2026-02-01 00:00:00';

// ID od którego zaczynamy skan artystów i albumów (direct scan)
$START_ARTIST_ID = 120771;
$START_ALBUM_ID  = 89700;

// Tabele legacy
$T_OLD_SONG        = 'Song';
$T_OLD_ALBUM       = 'Album';
$T_OLD_ARTIST_SONG = 'Artist_song';
$T_OLD_ALBUM_SONG  = 'Album_song';

// WP
$POST_TYPE_SONG = 'song';
$TAX_ARTIST     = 'artist';
$TAX_ALBUM      = 'album';

// Meta keys
$META_OLD_SONG_ID   = 'old_song_id';
$META_OLD_ARTIST_ID = 'old_artist_id';
$META_OLD_ALBUM_ID  = 'old_album_id';

// Meta mapy
$SONG_META_MAP = [
	'translation'      => 'translation',
	'analysis'         => 'analysis',
	'release_date'     => 'release_date',
	'producer'         => 'producer',
	'views'            => 'views',
	'genius_views'     => 'genius_views',
	'video_link'       => 'video_link',
	'created_user_id'  => 'created_user_id',
	'created_date'     => 'created_date',
	'last_views'       => 'last_views',
	'normalized_title' => 'normalized_title',
	'handle_url'       => null,
	'text'             => null, // text idzie do post_content, nie do postmeta
];

$ARTIST_META_MAP = [
	'start_place'     => 'start_place',
	'start_date'      => 'start_date',
	'end_date'        => 'end_date',
	'band'            => 'band',
	'genres'          => 'genres',
	'normalized_name' => 'normalized_name',
	'views'           => 'views',
	'handle_url'      => null,
];

$ALBUM_META_MAP = [
	'release_date'    => 'release_date',
	'record_place'    => 'record_place',
	'length'          => 'length',
	'record_label'    => 'record_label',
	'record_producer' => 'record_producer',
	'handle_url'      => null,
];

/** =============================
 *  UTIL
 *  ============================= */

function groove_now(): string {
	return date( 'Y-m-d H:i:s' );
}

function groove_log( string $msg ): void {
	echo '[' . groove_now() . '] ' . $msg . PHP_EOL;
}

function groove_table_exists( string $table ): bool {
	global $old_db;

	$like  = $old_db->esc_like( $table );
	$found = $old_db->get_var( $old_db->prepare( 'SHOW TABLES LIKE %s', $like ) );

	return ! empty( $found );
}

function groove_pick_first_existing_table( array $candidates ): ?string {
	foreach ( $candidates as $table ) {
		if ( groove_table_exists( $table ) ) {
			return $table;
		}
	}
	return null;
}

function groove_column_exists( string $table, string $column ): bool {
	global $old_db;

	$row = $old_db->get_var(
		$old_db->prepare(
			"SHOW COLUMNS FROM {$table} LIKE %s",
			$column
		)
	);

	return ! empty( $row );
}

function groove_detect_date_column( string $table ): ?string {
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
		if ( groove_column_exists( $table, $column ) ) {
			return $column;
		}
	}

	return null;
}

/**
 * @return array{rows: array<int,array<string,mixed>>, last_id: int}
 */
function groove_fetch_batch_by_date(
	string $table,
	string $date_column,
	string $start_date,
	int $last_id,
	int $limit
): array {
	global $old_db;

	$sql = "
		SELECT *
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
 * @return array{rows: array<int,array<string,mixed>>, last_id: int}
 */
function groove_fetch_batch_by_id(
	string $table,
	int $last_id,
	int $limit
): array {
	global $old_db;

	$sql = "
		SELECT *
		FROM {$table}
		WHERE id > %d
		ORDER BY id ASC
		LIMIT %d
	";

	$rows = $old_db->get_results(
		$old_db->prepare( $sql, $last_id, $limit ),
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
 * @param int[] $ids
 * @return array<int,array<string,mixed>>
 */
function groove_fetch_rows_by_ids( string $table, array $ids ): array {
	global $old_db;

	$ids = array_values( array_unique( array_map( 'intval', $ids ) ) );
	if ( empty( $ids ) ) {
		return [];
	}

	$placeholders = implode( ',', array_fill( 0, count( $ids ), '%d' ) );
	$sql          = "SELECT * FROM {$table} WHERE id IN ({$placeholders}) ORDER BY id ASC";

	return $old_db->get_results( $old_db->prepare( $sql, $ids ), ARRAY_A );
}

/**
 * @param int[] $values
 * @return array<int,int> old_id => post_id
 */
function groove_lookup_posts_by_meta_values( string $meta_key, array $values ): array {
	global $wpdb;

	$values = array_values( array_unique( array_map( 'intval', $values ) ) );
	if ( empty( $values ) ) {
		return [];
	}

	$placeholders = implode( ',', array_fill( 0, count( $values ), '%d' ) );

	$sql = "
		SELECT post_id, meta_value
		FROM {$wpdb->postmeta}
		WHERE meta_key = %s
		  AND meta_value IN ({$placeholders})
	";

	$params = array_merge( [ $meta_key ], $values );
	$rows   = $wpdb->get_results( $wpdb->prepare( $sql, $params ), ARRAY_A );

	$map = [];
	foreach ( $rows as $row ) {
		$map[ (int) $row['meta_value'] ] = (int) $row['post_id'];
	}

	return $map;
}

/**
 * @param int[] $values
 * @return array<int,int> old_id => term_id
 */
function groove_lookup_terms_by_meta_values( string $meta_key, array $values ): array {
	global $wpdb;

	$values = array_values( array_unique( array_map( 'intval', $values ) ) );
	if ( empty( $values ) ) {
		return [];
	}

	$placeholders = implode( ',', array_fill( 0, count( $values ), '%d' ) );

	$sql = "
		SELECT term_id, meta_value
		FROM {$wpdb->termmeta}
		WHERE meta_key = %s
		  AND meta_value IN ({$placeholders})
	";

	$params = array_merge( [ $meta_key ], $values );
	$rows   = $wpdb->get_results( $wpdb->prepare( $sql, $params ), ARRAY_A );

	$map = [];
	foreach ( $rows as $row ) {
		$map[ (int) $row['meta_value'] ] = (int) $row['term_id'];
	}

	return $map;
}

function groove_set_post_meta_if( int $post_id, string $meta_key, $value ): void {
	if ( $value === null || $value === '' ) {
		return;
	}
	update_post_meta( $post_id, $meta_key, $value );
}

function groove_set_term_meta_if( int $term_id, string $meta_key, $value ): void {
	if ( $value === null || $value === '' ) {
		return;
	}
	update_term_meta( $term_id, $meta_key, $value );
}

/**
 * Zwraca:
 * - null => brak dopasowania
 * - term_id => dokładnie jedno dopasowanie
 * - -1 => dopasowanie niejednoznaczne (więcej niż jeden term o tej samej nazwie w danej taksonomii)
 */
function groove_find_unique_term_id_by_exact_name_and_tax( string $name, string $taxonomy ): ?int {
	global $wpdb;

	$sql = "
		SELECT t.term_id
		FROM {$wpdb->terms} t
		INNER JOIN {$wpdb->term_taxonomy} tt
			ON tt.term_id = t.term_id
		WHERE t.name = %s
		  AND tt.taxonomy = %s
		ORDER BY t.term_id ASC
	";

	$rows = $wpdb->get_col(
		$wpdb->prepare( $sql, $name, $taxonomy )
	);

	if ( empty( $rows ) ) {
		return null;
	}

	$rows = array_values( array_unique( array_map( 'intval', $rows ) ) );

	if ( count( $rows ) > 1 ) {
		return -1;
	}

	return (int) $rows[0];
}

/**
 * @param int[] $song_ids
 * @return int[]
 */
function groove_get_artist_ids_for_song_ids( array $song_ids, string $table ): array {
	global $old_db;

	$song_ids = array_values( array_unique( array_map( 'intval', $song_ids ) ) );
	if ( empty( $song_ids ) ) {
		return [];
	}

	$placeholders = implode( ',', array_fill( 0, count( $song_ids ), '%d' ) );
	$sql          = "SELECT DISTINCT artist_id FROM {$table} WHERE song_id IN ({$placeholders})";
	$rows         = $old_db->get_col( $old_db->prepare( $sql, $song_ids ) );

	return array_values( array_unique( array_map( 'intval', $rows ) ) );
}

/**
 * @param int[] $song_ids
 * @return int[]
 */
function groove_get_album_ids_for_song_ids( array $song_ids, string $table ): array {
	global $old_db;

	$song_ids = array_values( array_unique( array_map( 'intval', $song_ids ) ) );
	if ( empty( $song_ids ) ) {
		return [];
	}

	$placeholders = implode( ',', array_fill( 0, count( $song_ids ), '%d' ) );
	$sql          = "SELECT DISTINCT album_id FROM {$table} WHERE song_id IN ({$placeholders})";
	$rows         = $old_db->get_col( $old_db->prepare( $sql, $song_ids ) );

	return array_values( array_unique( array_map( 'intval', $rows ) ) );
}

/**
 * @param int[] $ids
 * @param array<int,bool> $seen
 * @return int[]
 */
function groove_filter_unseen_ids( array $ids, array &$seen ): array {
	$out = [];

	foreach ( $ids as $id ) {
		$id = (int) $id;
		if ( $id <= 0 ) {
			continue;
		}
		if ( isset( $seen[ $id ] ) ) {
			continue;
		}
		$seen[ $id ] = true;
		$out[] = $id;
	}

	return $out;
}

function groove_save_artist_meta( int $term_id, string $taxonomy, array $row, array $meta_map ): void {
	if ( array_key_exists( 'description', $row ) && $row['description'] !== null && $row['description'] !== '' ) {
		wp_update_term( $term_id, $taxonomy, [ 'description' => (string) $row['description'] ] );
		update_term_meta( $term_id, 'description', (string) $row['description'] );
	}

	foreach ( $meta_map as $col => $meta_key ) {
		if ( $meta_key === null ) {
			continue;
		}
		if ( array_key_exists( $col, $row ) ) {
			groove_set_term_meta_if( $term_id, $meta_key, $row[ $col ] );
		}
	}
}

function groove_save_album_meta( int $term_id, array $row, array $meta_map ): void {
	foreach ( $meta_map as $col => $meta_key ) {
		if ( $meta_key === null ) {
			continue;
		}
		if ( array_key_exists( $col, $row ) ) {
			groove_set_term_meta_if( $term_id, $meta_key, $row[ $col ] );
		}
	}
}

/**
 * @param array<int,array<string,mixed>> $rows
 */
function groove_create_missing_songs(
	array $rows,
	bool $dry_run,
	string $post_type,
	string $meta_old_id,
	array $meta_map
): array {
	$created = 0;
	$exists  = 0;
	$errors  = 0;

	$old_ids = [];
	foreach ( $rows as $row ) {
		if ( isset( $row['id'] ) ) {
			$old_ids[] = (int) $row['id'];
		}
	}

	$exists_map = groove_lookup_posts_by_meta_values( $meta_old_id, $old_ids );

	foreach ( $rows as $row ) {
		$old_id = (int) $row['id'];
		$title  = isset( $row['title'] ) ? trim( (string) $row['title'] ) : '';

		if ( $title === '' ) {
			groove_log( "Songs: SKIP old_id={$old_id} (empty title)" );
			continue;
		}

		if ( isset( $exists_map[ $old_id ] ) ) {
			$exists++;
			continue;
		}

		if ( $dry_run ) {
			groove_log( "Songs: WOULD CREATE old_id={$old_id} title='{$title}'" );
			$created++;
			continue;
		}

		$content = '';
		if ( array_key_exists( 'text', $row ) && is_string( $row['text'] ) && trim( $row['text'] ) !== '' ) {
			$content = $row['text'];
		}

		$post_id = wp_insert_post(
			[
				'post_type'    => $post_type,
				'post_status'  => 'publish',
				'post_title'   => $title,
				'post_content' => $content,
			],
			true
		);

		if ( is_wp_error( $post_id ) ) {
			groove_log( "Songs: ERROR creating old_id={$old_id} title='{$title}' -> " . $post_id->get_error_message() );
			$errors++;
			continue;
		}

		$post_id = (int) $post_id;
		update_post_meta( $post_id, $meta_old_id, $old_id );

		foreach ( $meta_map as $col => $meta_key ) {
			if ( $meta_key === null ) {
				continue;
			}
			if ( array_key_exists( $col, $row ) ) {
				groove_set_post_meta_if( $post_id, $meta_key, $row[ $col ] );
			}
		}

		$created++;
	}

	return [
		'created' => $created,
		'exists'  => $exists,
		'errors'  => $errors,
	];
}

/**
 * @param array<int,array<string,mixed>> $rows
 */
function groove_create_missing_artists(
	array $rows,
	bool $dry_run,
	string $taxonomy,
	string $meta_old_id,
	array $meta_map
): array {
	$created = 0;
	$exists  = 0;
	$errors  = 0;
	$mapped  = 0;

	$old_ids = [];
	foreach ( $rows as $row ) {
		if ( isset( $row['id'] ) ) {
			$old_ids[] = (int) $row['id'];
		}
	}

	$exists_map = groove_lookup_terms_by_meta_values( $meta_old_id, $old_ids );

	foreach ( $rows as $row ) {
		$old_id = (int) $row['id'];
		$name   = isset( $row['name'] ) ? trim( (string) $row['name'] ) : '';

		if ( $name === '' ) {
			groove_log( "Artists: SKIP old_id={$old_id} (empty name)" );
			continue;
		}

		if ( isset( $exists_map[ $old_id ] ) ) {
			$exists++;
			continue;
		}

		$existing_term_id = groove_find_unique_term_id_by_exact_name_and_tax( $name, $taxonomy );

		if ( $existing_term_id === -1 ) {
			groove_log( "Artists: AMBIGUOUS old_id={$old_id} name='{$name}' -> multiple terms with same name in taxonomy '{$taxonomy}'" );
			$errors++;
			continue;
		}

		if ( $existing_term_id ) {
			$current_old_id = get_term_meta( $existing_term_id, $meta_old_id, true );

			if ( $current_old_id !== '' && (int) $current_old_id !== $old_id ) {
				groove_log( "Artists: CONFLICT old_id={$old_id} name='{$name}' -> term_id={$existing_term_id} already has {$meta_old_id}={$current_old_id}" );
				$errors++;
				continue;
			}

			if ( $dry_run ) {
				groove_log( "Artists: WOULD MAP EXISTING term_id={$existing_term_id} <- old_id={$old_id} name='{$name}'" );
				$mapped++;
				continue;
			}

			update_term_meta( $existing_term_id, $meta_old_id, $old_id );
			groove_save_artist_meta( $existing_term_id, $taxonomy, $row, $meta_map );
			$mapped++;
			continue;
		}

		if ( $dry_run ) {
			groove_log( "Artists: WOULD CREATE old_id={$old_id} name='{$name}'" );
			$created++;
			continue;
		}

		$insert = wp_insert_term( $name, $taxonomy );

		if ( is_wp_error( $insert ) ) {
			groove_log( "Artists: ERROR creating old_id={$old_id} name='{$name}' -> " . $insert->get_error_message() );
			$errors++;
			continue;
		}

		$term_id = (int) $insert['term_id'];
		update_term_meta( $term_id, $meta_old_id, $old_id );
		groove_save_artist_meta( $term_id, $taxonomy, $row, $meta_map );
		$created++;
	}

	return [
		'created' => $created,
		'mapped'  => $mapped,
		'exists'  => $exists,
		'errors'  => $errors,
	];
}

/**
 * @param array<int,array<string,mixed>> $rows
 */
function groove_create_missing_albums(
	array $rows,
	bool $dry_run,
	string $taxonomy,
	string $meta_old_id,
	array $meta_map
): array {
	$created = 0;
	$exists  = 0;
	$errors  = 0;
	$mapped  = 0;

	$old_ids = [];
	foreach ( $rows as $row ) {
		if ( isset( $row['id'] ) ) {
			$old_ids[] = (int) $row['id'];
		}
	}

	$exists_map = groove_lookup_terms_by_meta_values( $meta_old_id, $old_ids );

	foreach ( $rows as $row ) {
		$old_id = (int) $row['id'];
		$title  = isset( $row['title'] ) ? trim( (string) $row['title'] ) : '';

		if ( $title === '' ) {
			groove_log( "Albums: SKIP old_id={$old_id} (empty title)" );
			continue;
		}

		if ( isset( $exists_map[ $old_id ] ) ) {
			$exists++;
			continue;
		}

		$existing_term_id = groove_find_unique_term_id_by_exact_name_and_tax( $title, $taxonomy );

		if ( $existing_term_id === -1 ) {
			groove_log( "Albums: AMBIGUOUS old_id={$old_id} title='{$title}' -> multiple terms with same name in taxonomy '{$taxonomy}'" );
			$errors++;
			continue;
		}

		if ( $existing_term_id ) {
			$current_old_id = get_term_meta( $existing_term_id, $meta_old_id, true );

			if ( $current_old_id !== '' && (int) $current_old_id !== $old_id ) {
				groove_log( "Albums: CONFLICT old_id={$old_id} title='{$title}' -> term_id={$existing_term_id} already has {$meta_old_id}={$current_old_id}" );
				$errors++;
				continue;
			}

			if ( $dry_run ) {
				groove_log( "Albums: WOULD MAP EXISTING term_id={$existing_term_id} <- old_id={$old_id} title='{$title}'" );
				$mapped++;
				continue;
			}

			update_term_meta( $existing_term_id, $meta_old_id, $old_id );
			groove_save_album_meta( $existing_term_id, $row, $meta_map );
			$mapped++;
			continue;
		}

		if ( $dry_run ) {
			groove_log( "Albums: WOULD CREATE old_id={$old_id} title='{$title}'" );
			$created++;
			continue;
		}

		$insert = wp_insert_term( $title, $taxonomy );

		if ( is_wp_error( $insert ) ) {
			groove_log( "Albums: ERROR creating old_id={$old_id} title='{$title}' -> " . $insert->get_error_message() );
			$errors++;
			continue;
		}

		$term_id = (int) $insert['term_id'];
		update_term_meta( $term_id, $meta_old_id, $old_id );
		groove_save_album_meta( $term_id, $row, $meta_map );
		$created++;
	}

	return [
		'created' => $created,
		'mapped'  => $mapped,
		'exists'  => $exists,
		'errors'  => $errors,
	];
}

/** =============================
 *  WALIDACJE STARTOWE
 *  ============================= */

$T_OLD_ARTISTS = groove_pick_first_existing_table( [ 'Artists', 'Artist' ] );

groove_log( '=== sync-fresh-entities-since-feb START ===' );
groove_log( 'DRY_RUN: ' . ( $DRY_RUN ? 'true (no writes)' : 'false (WILL WRITE)' ) );
groove_log( 'START_DATE: ' . $START_DATE );
groove_log( 'BATCH: ' . $BATCH );

if ( ! $T_OLD_ARTISTS ) {
	groove_log( "ERROR: neither 'artists' nor 'artist' table exists in DB." );
	exit( 1 );
}

groove_log( "Using artists table: {$T_OLD_ARTISTS}" );

foreach ( [ $T_OLD_SONG, $T_OLD_ARTISTS, $T_OLD_ALBUM, $T_OLD_ARTIST_SONG, $T_OLD_ALBUM_SONG ] as $table ) {
	if ( ! groove_table_exists( $table ) ) {
		groove_log( "ERROR: table '{$table}' not found in DB." );
		exit( 1 );
	}
}

$song_date_col = groove_detect_date_column( $T_OLD_SONG );
if ( ! $song_date_col ) {
	groove_log( "ERROR: table '{$T_OLD_SONG}' does not have a recognized created_* date column." );
	exit( 1 );
}

if ( ! taxonomy_exists( $TAX_ARTIST ) ) {
	groove_log( "ERROR: taxonomy '{$TAX_ARTIST}' does not exist." );
	exit( 1 );
}

if ( ! taxonomy_exists( $TAX_ALBUM ) ) {
	groove_log( "ERROR: taxonomy '{$TAX_ALBUM}' does not exist." );
	exit( 1 );
}

if ( ! get_post_type_object( $POST_TYPE_SONG ) ) {
	groove_log( "ERROR: post_type '{$POST_TYPE_SONG}' does not exist." );
	exit( 1 );
}

groove_log( 'Detected song date column: ' . $song_date_col );
groove_log( 'START_ARTIST_ID: ' . $START_ARTIST_ID );
groove_log( 'START_ALBUM_ID: ' . $START_ALBUM_ID );

/** =============================
 *  RUN
 *  ============================= */

$summary = [
	'songs' => [
		'checked' => 0,
		'created' => 0,
		'exists'  => 0,
		'errors'  => 0,
	],
	'artists_from_songs' => [
		'checked' => 0,
		'created' => 0,
		'mapped'  => 0,
		'exists'  => 0,
		'errors'  => 0,
	],
	'albums_from_songs' => [
		'checked' => 0,
		'created' => 0,
		'mapped'  => 0,
		'exists'  => 0,
		'errors'  => 0,
	],
	'artists_direct' => [
		'checked' => 0,
		'created' => 0,
		'mapped'  => 0,
		'exists'  => 0,
		'errors'  => 0,
	],
	'albums_direct' => [
		'checked' => 0,
		'created' => 0,
		'mapped'  => 0,
		'exists'  => 0,
		'errors'  => 0,
	],
];

$seen_artist_ids = [];
$seen_album_ids  = [];

groove_log( '--- STEP 1/3: fresh SONGS + linked ARTISTS/ALBUMS ---' );

$last_song_id = 0;

while ( true ) {
	$pack = groove_fetch_batch_by_date(
		$T_OLD_SONG,
		$song_date_col,
		$START_DATE,
		$last_song_id,
		$BATCH
	);

	$rows         = $pack['rows'];
	$last_song_id = $pack['last_id'];

	if ( empty( $rows ) ) {
		break;
	}

	$summary['songs']['checked'] += count( $rows );

	$song_result = groove_create_missing_songs(
		$rows,
		$DRY_RUN,
		$POST_TYPE_SONG,
		$META_OLD_SONG_ID,
		$SONG_META_MAP
	);

	$summary['songs']['created'] += $song_result['created'];
	$summary['songs']['exists']  += $song_result['exists'];
	$summary['songs']['errors']  += $song_result['errors'];

	$song_ids = [];
	foreach ( $rows as $row ) {
		$song_ids[] = (int) $row['id'];
	}

	$artist_ids = groove_get_artist_ids_for_song_ids( $song_ids, $T_OLD_ARTIST_SONG );
	$album_ids  = groove_get_album_ids_for_song_ids( $song_ids, $T_OLD_ALBUM_SONG );

	$artist_ids = groove_filter_unseen_ids( $artist_ids, $seen_artist_ids );
	$album_ids  = groove_filter_unseen_ids( $album_ids, $seen_album_ids );

	if ( ! empty( $artist_ids ) ) {
		$artist_rows = groove_fetch_rows_by_ids( $T_OLD_ARTISTS, $artist_ids );
		$summary['artists_from_songs']['checked'] += count( $artist_rows );

		$artist_result = groove_create_missing_artists(
			$artist_rows,
			$DRY_RUN,
			$TAX_ARTIST,
			$META_OLD_ARTIST_ID,
			$ARTIST_META_MAP
		);

		$summary['artists_from_songs']['created'] += $artist_result['created'];
		$summary['artists_from_songs']['mapped']  += $artist_result['mapped'];
		$summary['artists_from_songs']['exists']  += $artist_result['exists'];
		$summary['artists_from_songs']['errors']  += $artist_result['errors'];
	}

	if ( ! empty( $album_ids ) ) {
		$album_rows = groove_fetch_rows_by_ids( $T_OLD_ALBUM, $album_ids );
		$summary['albums_from_songs']['checked'] += count( $album_rows );

		$album_result = groove_create_missing_albums(
			$album_rows,
			$DRY_RUN,
			$TAX_ALBUM,
			$META_OLD_ALBUM_ID,
			$ALBUM_META_MAP
		);

		$summary['albums_from_songs']['created'] += $album_result['created'];
		$summary['albums_from_songs']['mapped']  += $album_result['mapped'];
		$summary['albums_from_songs']['exists']  += $album_result['exists'];
		$summary['albums_from_songs']['errors']  += $album_result['errors'];
	}

	groove_log(
		'Progress songs batch: checked=' . $summary['songs']['checked'] .
		', songs_created=' . $summary['songs']['created'] .
		', artists_created=' . $summary['artists_from_songs']['created'] .
		', artists_mapped=' . $summary['artists_from_songs']['mapped'] .
		', albums_created=' . $summary['albums_from_songs']['created'] .
		', albums_mapped=' . $summary['albums_from_songs']['mapped']
	);
}

groove_log( '--- STEP 2/3: direct ARTISTS by ID ---' );

$last_artist_id = $START_ARTIST_ID;

while ( true ) {
	$pack = groove_fetch_batch_by_id(
		$T_OLD_ARTISTS,
		$last_artist_id,
		$BATCH
	);

	$rows           = $pack['rows'];
	$last_artist_id = $pack['last_id'];

	if ( empty( $rows ) ) {
		break;
	}

	$summary['artists_direct']['checked'] += count( $rows );

	$result = groove_create_missing_artists(
		$rows,
		$DRY_RUN,
		$TAX_ARTIST,
		$META_OLD_ARTIST_ID,
		$ARTIST_META_MAP
	);

	$summary['artists_direct']['created'] += $result['created'];
	$summary['artists_direct']['mapped']  += $result['mapped'];
	$summary['artists_direct']['exists']  += $result['exists'];
	$summary['artists_direct']['errors']  += $result['errors'];
}

groove_log( '--- STEP 3/3: direct ALBUMS by ID ---' );

$last_album_id = $START_ALBUM_ID;

while ( true ) {
	$pack = groove_fetch_batch_by_id(
		$T_OLD_ALBUM,
		$last_album_id,
		$BATCH
	);

	$rows          = $pack['rows'];
	$last_album_id = $pack['last_id'];

	if ( empty( $rows ) ) {
		break;
	}

	$summary['albums_direct']['checked'] += count( $rows );

	$result = groove_create_missing_albums(
		$rows,
		$DRY_RUN,
		$TAX_ALBUM,
		$META_OLD_ALBUM_ID,
		$ALBUM_META_MAP
	);

	$summary['albums_direct']['created'] += $result['created'];
	$summary['albums_direct']['mapped']  += $result['mapped'];
	$summary['albums_direct']['exists']  += $result['exists'];
	$summary['albums_direct']['errors']  += $result['errors'];
}

/** =============================
 *  SUMMARY
 *  ============================= */

groove_log( '=== SUMMARY ===' );
foreach ( $summary as $section => $stats ) {
	$parts = [];
	foreach ( $stats as $key => $value ) {
		$parts[] = "{$key}={$value}";
	}
	groove_log( strtoupper( $section ) . ': ' . implode( ', ', $parts ) );
}

groove_log( '=== DONE ===' );
if ( $DRY_RUN ) {
	groove_log( 'DRY_RUN=true -> no changes were written. Set $DRY_RUN=false after verification.' );
}