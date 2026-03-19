<?php
/**
 * Tworzy globalne $old_db — drugie połączenie wpdb do bazy z tabelami old_*.
 *
 * Wymaga: WordPress (ABSPATH) musi być już załadowany.
 * Użycie: require_once __DIR__ . '/old-db.php';
 *         global $old_db;
 */

if ( isset( $GLOBALS['old_db'] ) ) {
	return;
}

$__old_db_cfg = require __DIR__ . '/db-config.php';

$host = $__old_db_cfg['host'];
if ( ! empty( $__old_db_cfg['port'] ) && (int) $__old_db_cfg['port'] !== 3306 ) {
	$host .= ':' . (int) $__old_db_cfg['port'];
}

$GLOBALS['old_db'] = new wpdb(
	$__old_db_cfg['user'],
	$__old_db_cfg['password'],
	$__old_db_cfg['name'],
	$host
);

$GLOBALS['old_db']->set_charset( $GLOBALS['old_db']->dbh, $__old_db_cfg['charset'] );

unset( $__old_db_cfg, $host );
