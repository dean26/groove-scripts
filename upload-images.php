<?php
set_time_limit(0);

if (!defined('ABSPATH')) {
    fwrite(STDERR, "❌ Ten plik odpalaj przez: wp eval-file upload-images.php\n");
    exit(1);
}

global $wpdb;
$wpdb->hide_errors();

require_once __DIR__ . '/old-db.php';
global $old_db;
$old_db->hide_errors();

const BATCH_SIZE = 300;
const LEGACY_META_KEY = 'legacy_file_id';
const SLEEP_USEC = 150000;

/**
 * DRY RUN:
 * 1) PEWNIAK: GROOVE_DRY_RUN=1 wp eval-file upload-images.php
 * 2) (jeśli działa u Ciebie): wp eval-file upload-images.php -- --dry-run
 */
$args = $_SERVER['argv'] ?? [];
$dryRun = true;

// env var (pewniak)
$envDry = getenv('GROOVE_DRY_RUN');
if ($envDry !== false && $envDry !== '' && $envDry !== '0') {
    $dryRun = true;
}

// argv (fallback, jeśli WP-CLI jednak przekaże)
if (!$dryRun && in_array('--dry-run', $args, true)) {
    $dryRun = true;
}

// Sanity check: czy tabela file istnieje
$oldFileTable = $old_db->get_var($old_db->prepare("SHOW TABLES LIKE %s", 'File'));
if (!$oldFileTable) {
    fwrite(STDERR, "❌ Nie widzę tabeli `file` w bazie produkcyjnej. Przerywam.\n");
    exit(1);
}

function log_msg($msg) {
    echo "[" . date('H:i:s') . "] $msg\n";
}

function guess_mime($file) {
    $ext = strtolower(pathinfo($file, PATHINFO_EXTENSION));
    return match ($ext) {
        'jpg', 'jpeg' => 'image/jpeg',
        'png' => 'image/png',
        'webp' => 'image/webp',
        'gif' => 'image/gif',
        'svg' => 'image/svg+xml',
        default => 'image/jpeg',
    };
}

function upload_dir_for_section($section) {
    return match ($section) {
        2 => 'legacy/albums',
        3 => 'legacy/artist',
        default => 'legacy',
    };
}

function create_attachment_if_needed($fileId, $filename, $section, $dryRun) {
    global $wpdb;

    $existing = $wpdb->get_var(
        $wpdb->prepare(
            "SELECT post_id
             FROM {$wpdb->postmeta}
             WHERE meta_key=%s AND meta_value=%d
             LIMIT 1",
            LEGACY_META_KEY,
            $fileId
        )
    );

    if ($existing) return (int)$existing;
    if ($dryRun) return null;

    $sub = upload_dir_for_section($section);
    $rel = $sub . '/' . $filename;
    $fullPath = WP_CONTENT_DIR . '/uploads/' . $rel;

    if (!file_exists($fullPath)) {
        log_msg("⚠️ Brak pliku fizycznego: $fullPath");
    }

    $url = home_url('/wp-content/uploads/' . $rel);

    $id = wp_insert_post([
        'post_title'     => pathinfo($filename, PATHINFO_FILENAME),
        'post_status'    => 'inherit',
        'post_type'      => 'attachment',
        'post_mime_type' => guess_mime($filename),
        'guid'           => $url,
    ], true);

    if (is_wp_error($id) || !$id) {
        log_msg("❌ wp_insert_post error dla file_id={$fileId}, name={$filename}");
        return null;
    }

    add_post_meta($id, '_wp_attached_file', $rel, true);
    add_post_meta($id, LEGACY_META_KEY, $fileId, true);

    return (int)$id;
}

log_msg("START SMART MIGRATION (ALBUM+ARTIST only)");
log_msg("DRY_RUN=" . ($dryRun ? 'TRUE' : 'FALSE'));

//////////////////////////////////////////////////
// ALBUMS (KEYSET)
//////////////////////////////////////////////////
$lastAlbumTermId = 89292;

while (true) {
    $albums = $wpdb->get_results(
        $wpdb->prepare("
            SELECT tm.term_id, tm.meta_value as old_id
            FROM {$wpdb->termmeta} tm
            JOIN {$wpdb->term_taxonomy} tt
                ON tt.term_id=tm.term_id AND tt.taxonomy='album'
            LEFT JOIN {$wpdb->termmeta} img
                ON img.term_id=tm.term_id AND img.meta_key='groove_term_image'
            WHERE tm.meta_key='old_album_id'
              AND (img.term_id IS NULL OR img.meta_value='' OR img.meta_value='0')
              AND tm.term_id > %d
            ORDER BY tm.term_id ASC
            LIMIT %d
        ", $lastAlbumTermId, BATCH_SIZE)
    );

    if (empty($albums)) break;

    foreach ($albums as $row) {
        $lastAlbumTermId = (int)$row->term_id;

        $file = $old_db->get_row(
            $old_db->prepare(
                "SELECT id, name
                 FROM File
                 WHERE section=2 AND item_id=%d AND main=1
                   AND name IS NOT NULL AND name <> ''
                 LIMIT 1",
                (int)$row->old_id
            )
        );

        if (!$file) continue;

        $attachment = create_attachment_if_needed((int)$file->id, (string)$file->name, 2, $dryRun);
        if (!$attachment && !$dryRun) continue;

        if (!$dryRun) {
            update_term_meta((int)$row->term_id, 'groove_term_image', $attachment);
        }

        log_msg("ALBUM {$row->term_id} ← {$attachment}");
    }

    usleep(SLEEP_USEC);
}

//////////////////////////////////////////////////
// ARTISTS (KEYSET)
//////////////////////////////////////////////////
$lastArtistTermId = 120431;

while (true) {
    $artists = $wpdb->get_results(
        $wpdb->prepare("
            SELECT tm.term_id, tm.meta_value as old_id
            FROM {$wpdb->termmeta} tm
            JOIN {$wpdb->term_taxonomy} tt
                ON tt.term_id=tm.term_id AND tt.taxonomy='artist'
            LEFT JOIN {$wpdb->termmeta} img
                ON img.term_id=tm.term_id AND img.meta_key='groove_term_image'
            WHERE tm.meta_key='old_artist_id'
              AND (img.term_id IS NULL OR img.meta_value='' OR img.meta_value='0')
              AND tm.term_id > %d
            ORDER BY tm.term_id ASC
            LIMIT %d
        ", $lastArtistTermId, BATCH_SIZE)
    );

    if (empty($artists)) break;

    foreach ($artists as $row) {
        $lastArtistTermId = (int)$row->term_id;

        $file = $old_db->get_row(
            $old_db->prepare(
                "SELECT id, name
                 FROM File
                 WHERE section=3 AND item_id=%d AND main=1
                   AND name IS NOT NULL AND name <> ''
                 LIMIT 1",
                (int)$row->old_id
            )
        );

        if (!$file) continue;

        $attachment = create_attachment_if_needed((int)$file->id, (string)$file->name, 3, $dryRun);
        if (!$attachment && !$dryRun) continue;

        if (!$dryRun) {
            update_term_meta((int)$row->term_id, 'groove_term_image', $attachment);
        }

        log_msg("ARTIST {$row->term_id} ← {$attachment}");
    }

    usleep(SLEEP_USEC);
}

log_msg("KONIEC");