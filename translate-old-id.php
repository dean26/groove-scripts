<?php
if (!defined('ABSPATH')) {
    exit;
}

/**
 * Konfiguracja mapowania starych ID -> nowych ID w WordPressie.
 *
 * UWAGA:
 * W bazie istnieją "brudne" rekordy termmeta, np. old_artist_id występuje
 * także przy taxonomy = album i genres, dlatego WSZĘDZIE filtrujemy po taxonomy.
 */
function groove_legacy_id_map_config() {
    return [
        'song' => [
            'object_kind' => 'post',
            'post_type'   => 'song',
            'meta_key'    => 'old_song_id',
        ],
        'news' => [
            'object_kind' => 'post',
            'post_type'   => 'post',
            'meta_key'    => 'old_news_id',
        ],
        'post' => [
            'object_kind' => 'post',
            'post_type'   => 'post',
            'meta_key'    => 'old_news_id',
        ],
        'artist' => [
            'object_kind' => 'term',
            'taxonomy'    => 'artist',
            'meta_key'    => 'old_artist_id',
        ],
        'album' => [
            'object_kind' => 'term',
            'taxonomy'    => 'album',
            'meta_key'    => 'old_album_id',
        ],
        'genre' => [
            'object_kind' => 'term',
            'taxonomy'    => 'genres',
            'meta_key'    => 'old_genre_id',
        ],
        'genres' => [
            'object_kind' => 'term',
            'taxonomy'    => 'genres',
            'meta_key'    => 'old_genre_id',
        ],
    ];
}

/**
 * Normalizuje typ obiektu.
 *
 * @param string $object_type
 * @return string
 */
function groove_normalize_legacy_object_type($object_type) {
    $object_type = sanitize_key((string) $object_type);

    $aliases = [
        'songs'   => 'song',
        'artists' => 'artist',
        'albums'  => 'album',
        'posts'   => 'post',
        'news'    => 'news',
        'genre'   => 'genre',
        'genres'  => 'genres',
    ];

    return $aliases[$object_type] ?? $object_type;
}

/**
 * Zwraca nowy WP ID/term_id dla starego ID.
 *
 * @param int|string $legacy_id
 * @param string     $object_type song|artist|album|genre|genres|news|post
 * @return int|null
 */
function groove_translate_legacy_id_to_wp_id($legacy_id, $object_type) {
    global $wpdb;

    $legacy_id   = (int) $legacy_id;
    $object_type = groove_normalize_legacy_object_type($object_type);

    if ($legacy_id <= 0) {
        return null;
    }

    $config_map = groove_legacy_id_map_config();
    if (!isset($config_map[$object_type])) {
        return null;
    }

    $config      = $config_map[$object_type];
    $cache_group = 'groove_legacy_id_translate';
    $cache_key   = 'single:' . $object_type . ':' . $legacy_id;

    $cached = wp_cache_get($cache_key, $cache_group);
    if ($cached !== false) {
        return $cached > 0 ? (int) $cached : null;
    }

    $new_id = null;

    if ($config['object_kind'] === 'post') {
        $sql = "
            SELECT p.ID
            FROM {$wpdb->posts} p
            INNER JOIN {$wpdb->postmeta} pm
                ON pm.post_id = p.ID
            WHERE pm.meta_key = %s
              AND pm.meta_value = %s
              AND p.post_type = %s
            ORDER BY p.ID ASC
            LIMIT 1
        ";

        $new_id = $wpdb->get_var(
            $wpdb->prepare(
                $sql,
                $config['meta_key'],
                (string) $legacy_id,
                $config['post_type']
            )
        );
    } elseif ($config['object_kind'] === 'term') {
        $sql = "
            SELECT t.term_id
            FROM {$wpdb->terms} t
            INNER JOIN {$wpdb->term_taxonomy} tt
                ON tt.term_id = t.term_id
            INNER JOIN {$wpdb->termmeta} tm
                ON tm.term_id = t.term_id
            WHERE tm.meta_key = %s
              AND tm.meta_value = %s
              AND tt.taxonomy = %s
            ORDER BY t.term_id ASC
            LIMIT 1
        ";

        $new_id = $wpdb->get_var(
            $wpdb->prepare(
                $sql,
                $config['meta_key'],
                (string) $legacy_id,
                $config['taxonomy']
            )
        );
    }

    $new_id = $new_id ? (int) $new_id : null;

    wp_cache_set($cache_key, $new_id ?: 0, $cache_group, HOUR_IN_SECONDS);

    return $new_id;
}

/**
 * Hurtowe tłumaczenie starych ID -> nowych ID.
 *
 * Zwraca tablicę:
 * [
 *   123 => 456,
 *   124 => null,
 * ]
 *
 * @param array  $legacy_ids
 * @param string $object_type
 * @return array<int,int|null>
 */
function groove_translate_legacy_ids_to_wp_ids(array $legacy_ids, $object_type) {
    global $wpdb;

    $object_type = groove_normalize_legacy_object_type($object_type);
    $config_map  = groove_legacy_id_map_config();

    if (!isset($config_map[$object_type])) {
        return [];
    }

    $config = $config_map[$object_type];

    $legacy_ids = array_map('intval', $legacy_ids);
    $legacy_ids = array_filter($legacy_ids, static function ($id) {
        return $id > 0;
    });
    $legacy_ids = array_values(array_unique($legacy_ids));

    if (empty($legacy_ids)) {
        return [];
    }

    $result      = [];
    $missing_ids = [];
    $cache_group = 'groove_legacy_id_translate';

    foreach ($legacy_ids as $legacy_id) {
        $cache_key = 'single:' . $object_type . ':' . $legacy_id;
        $cached    = wp_cache_get($cache_key, $cache_group);

        if ($cached !== false) {
            $result[$legacy_id] = $cached > 0 ? (int) $cached : null;
        } else {
            $result[$legacy_id] = null;
            $missing_ids[]      = $legacy_id;
        }
    }

    if (empty($missing_ids)) {
        return $result;
    }

    $placeholders = implode(',', array_fill(0, count($missing_ids), '%s'));
    $rows         = [];

    if ($config['object_kind'] === 'post') {
        $sql = "
            SELECT CAST(pm.meta_value AS UNSIGNED) AS legacy_id, MIN(p.ID) AS new_id
            FROM {$wpdb->posts} p
            INNER JOIN {$wpdb->postmeta} pm
                ON pm.post_id = p.ID
            WHERE pm.meta_key = %s
              AND pm.meta_value IN ($placeholders)
              AND p.post_type = %s
            GROUP BY CAST(pm.meta_value AS UNSIGNED)
        ";

        $params = array_merge(
            [$config['meta_key']],
            array_map('strval', $missing_ids),
            [$config['post_type']]
        );

        $rows = $wpdb->get_results($wpdb->prepare($sql, ...$params), ARRAY_A);
    } elseif ($config['object_kind'] === 'term') {
        $sql = "
            SELECT CAST(tm.meta_value AS UNSIGNED) AS legacy_id, MIN(t.term_id) AS new_id
            FROM {$wpdb->terms} t
            INNER JOIN {$wpdb->term_taxonomy} tt
                ON tt.term_id = t.term_id
            INNER JOIN {$wpdb->termmeta} tm
                ON tm.term_id = t.term_id
            WHERE tm.meta_key = %s
              AND tm.meta_value IN ($placeholders)
              AND tt.taxonomy = %s
            GROUP BY CAST(tm.meta_value AS UNSIGNED)
        ";

        $params = array_merge(
            [$config['meta_key']],
            array_map('strval', $missing_ids),
            [$config['taxonomy']]
        );

        $rows = $wpdb->get_results($wpdb->prepare($sql, ...$params), ARRAY_A);
    }

    if (!empty($rows)) {
        foreach ($rows as $row) {
            $legacy_id = isset($row['legacy_id']) ? (int) $row['legacy_id'] : 0;
            $new_id    = isset($row['new_id']) ? (int) $row['new_id'] : 0;

            if ($legacy_id > 0 && $new_id > 0) {
                $result[$legacy_id] = $new_id;
            }
        }
    }

    foreach ($missing_ids as $legacy_id) {
        $cache_key = 'single:' . $object_type . ':' . $legacy_id;
        wp_cache_set(
            $cache_key,
            !empty($result[$legacy_id]) ? (int) $result[$legacy_id] : 0,
            $cache_group,
            HOUR_IN_SECONDS
        );
    }

    return $result;
}

/**
 * Tłumaczy nowy WP ID/term_id na stare legacy ID.
 *
 * @param int|string $wp_id
 * @param string     $object_type
 * @return int|null
 */
function groove_translate_wp_id_to_legacy_id($wp_id, $object_type) {
    global $wpdb;

    $wp_id       = (int) $wp_id;
    $object_type = groove_normalize_legacy_object_type($object_type);

    if ($wp_id <= 0) {
        return null;
    }

    $config_map = groove_legacy_id_map_config();
    if (!isset($config_map[$object_type])) {
        return null;
    }

    $config      = $config_map[$object_type];
    $cache_group = 'groove_legacy_id_translate';
    $cache_key   = 'reverse:' . $object_type . ':' . $wp_id;

    $cached = wp_cache_get($cache_key, $cache_group);
    if ($cached !== false) {
        return $cached > 0 ? (int) $cached : null;
    }

    $legacy_id = null;

    if ($config['object_kind'] === 'post') {
        $sql = "
            SELECT pm.meta_value
            FROM {$wpdb->postmeta} pm
            INNER JOIN {$wpdb->posts} p
                ON p.ID = pm.post_id
            WHERE pm.post_id = %d
              AND pm.meta_key = %s
              AND p.post_type = %s
            LIMIT 1
        ";

        $legacy_id = $wpdb->get_var(
            $wpdb->prepare(
                $sql,
                $wp_id,
                $config['meta_key'],
                $config['post_type']
            )
        );
    } elseif ($config['object_kind'] === 'term') {
        $sql = "
            SELECT tm.meta_value
            FROM {$wpdb->termmeta} tm
            INNER JOIN {$wpdb->term_taxonomy} tt
                ON tt.term_id = tm.term_id
            WHERE tm.term_id = %d
              AND tm.meta_key = %s
              AND tt.taxonomy = %s
            LIMIT 1
        ";

        $legacy_id = $wpdb->get_var(
            $wpdb->prepare(
                $sql,
                $wp_id,
                $config['meta_key'],
                $config['taxonomy']
            )
        );
    }

    $legacy_id = ($legacy_id !== null && $legacy_id !== '') ? (int) $legacy_id : null;

    wp_cache_set($cache_key, $legacy_id ?: 0, $cache_group, HOUR_IN_SECONDS);

    return $legacy_id;
}

/**
 * Helpers - pojedyncze mapowania old -> new
 */
function groove_get_new_song_id($old_song_id) {
    return groove_translate_legacy_id_to_wp_id($old_song_id, 'song');
}

function groove_get_new_news_id($old_news_id) {
    return groove_translate_legacy_id_to_wp_id($old_news_id, 'news');
}

function groove_get_new_artist_id($old_artist_id) {
    return groove_translate_legacy_id_to_wp_id($old_artist_id, 'artist');
}

function groove_get_new_album_id($old_album_id) {
    return groove_translate_legacy_id_to_wp_id($old_album_id, 'album');
}

function groove_get_new_genre_id($old_genre_id) {
    return groove_translate_legacy_id_to_wp_id($old_genre_id, 'genre');
}

/**
 * Helpers - pojedyncze mapowania new -> old
 */
function groove_get_old_song_id($song_id) {
    return groove_translate_wp_id_to_legacy_id($song_id, 'song');
}

function groove_get_old_news_id($post_id) {
    return groove_translate_wp_id_to_legacy_id($post_id, 'news');
}

function groove_get_old_artist_id($artist_term_id) {
    return groove_translate_wp_id_to_legacy_id($artist_term_id, 'artist');
}

function groove_get_old_album_id($album_term_id) {
    return groove_translate_wp_id_to_legacy_id($album_term_id, 'album');
}

function groove_get_old_genre_id($genre_term_id) {
    return groove_translate_wp_id_to_legacy_id($genre_term_id, 'genre');
}