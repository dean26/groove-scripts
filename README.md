Kolejność:

sync-map-existing-albums.php
sync-map-existing-artists.php
sync-missing-entities.php
upload-images.php
sync-song-last-view.php
aggregate_backflip.php
GROOVE_DRY_RUN=1 GROOVE_FORCE_WEEKS="2026-W05,2026-W06,2026-W07,2026-W08,2026-W09,2026-W10,2026-W11,2026-W12,2026-W13,2026-W14" GROOVE_FORCE_MONTHS="2026-02,2026-03" GROOVE_FORCE_YEARS="2026" GROOVE_FORCE_ALL=1 php8.3 wp-cli.phar eval-file import-scripts/stats2.php


DELETE FROM wp_ranking_aggregate WHERE period_type = 'day' AND period_value BETWEEN '2026-02-01' AND '2026-03-31';

DELETE FROM wp_ranking_aggregate
WHERE period_type = 'month'
AND period_value BETWEEN '2026-02' AND '2026-03';

DELETE FROM `wp_ranking_aggregate`
WHERE `period_type` = 'week'
AND `period_value` BETWEEN '2026-W05' AND '2026-W14';