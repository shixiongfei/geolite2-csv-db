/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;

-- Dumping structure for procedure geolite2.get_geoip
DROP PROCEDURE IF EXISTS `get_geoip`;
DELIMITER //
CREATE PROCEDURE `get_geoip`(
	IN `ip` VARCHAR(50)
,
	IN `locale` VARCHAR(50)
)
BEGIN

	DECLARE v_sql VARCHAR(1024);
	DECLARE ip_d DECIMAL(39, 0);
	DECLARE ip_s DECIMAL(39, 0);
	DECLARE ip_t CHAR(4);
	DECLARE geoid INT UNSIGNED DEFAULT 0;

	SET ip_d = ip_to_d(ip);
	
	IF IS_IPV4(ip) THEN
		SET ip_t = 'ipv4';
	END IF;
	
	IF IS_IPV6(ip) THEN
		SET ip_t = 'ipv6';
	END IF;

    SELECT
        `geoname_id`, `ip_start` INTO geoid, ip_s
    FROM `t_blocks_ip`
    WHERE `ip_end` >= ip_d AND `ip_type` = ip_t
    LIMIT 1;

    SET v_sql = concat(
        'SELECT ',
        'a.`network`, a.`ip_type`, a.`postal_code`, a.`latitude`, a.`longitude`, a.`accuracy_radius`, ',
        'b.`locale_code`, b.`continent_code`, b.`continent_name`, b.`country_iso_code`, b.`country_name`, ',
        'b.`city_name`, c.`provider`, b.`metro_code`, b.`time_zone` ',
        'FROM `t_blocks_ip` AS a LEFT JOIN `t_locations` AS b ON a.`geoname_id` = b.`geoname_id` ',
        'LEFT JOIN `t_providers` AS c ON ', ip_d, ' BETWEEN c.`ip_start` AND c.`ip_end` ',
        'WHERE a.`geoname_id` = ', geoid, ' AND a.`ip_start` = ', ip_s,' '
    );
	
	IF locale IS NOT NULL THEN
		SET v_sql = concat(v_sql, 'AND b.`locale_code` = \'', locale, '\' ');
	END IF;
	
	SET @sqlcmd = v_sql;
	
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;

END//
DELIMITER ;

-- Dumping structure for function geolite2.ip_to_d
DROP FUNCTION IF EXISTS `ip_to_d`;
DELIMITER //
CREATE FUNCTION `ip_to_d`(
	`ip` VARCHAR(50)
) RETURNS decimal(39,0)
BEGIN

	IF IS_IPV4(ip) THEN
		return INET_ATON(ip);
	END IF;
	
	IF IS_IPV6(ip) THEN
		return CAST(CONV(SUBSTR(HEX(INET6_ATON(ip)), 1, 16), 16, 10) AS DECIMAL(39, 0)) * 18446744073709551616 +
					CAST(CONV(SUBSTR(HEX(INET6_ATON(ip)), 17, 16), 16, 10) AS DECIMAL(39, 0));
	END IF;
	
	RETURN NULL;

END//
DELIMITER ;

-- Dumping structure for table geolite2.t_blocks_ip
DROP TABLE IF EXISTS `t_blocks_ip`;
CREATE TABLE IF NOT EXISTS `t_blocks_ip` (
  `network` varchar(64) NOT NULL,
  `ip_type` varchar(8) NOT NULL,
  `ip_start` decimal(39,0) NOT NULL,
  `ip_end` decimal(39,0) NOT NULL,
  `geoname_id` int(10) unsigned DEFAULT NULL,
  `registered_country_geoname_id` int(10) unsigned DEFAULT NULL,
  `represented_country_geoname_id` int(10) unsigned DEFAULT NULL,
  `is_anonymous_proxy` tinyint(4) DEFAULT NULL,
  `is_satellite_provider` tinyint(4) DEFAULT NULL,
  `postal_code` varchar(16) DEFAULT NULL,
  `latitude` double DEFAULT NULL,
  `longitude` double DEFAULT NULL,
  `accuracy_radius` double DEFAULT NULL,
  `ver_num` int(10) unsigned NOT NULL,
  PRIMARY KEY (`ip_start`),
  INDEX `ip_end` (`ip_end`),
  INDEX `geoname_id` (`geoname_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Data exporting was unselected.
-- Dumping structure for table geolite2.t_locations
DROP TABLE IF EXISTS `t_locations`;
CREATE TABLE IF NOT EXISTS `t_locations` (
  `geoname_id` int(10) unsigned NOT NULL,
  `locale_code` varchar(16) NOT NULL,
  `continent_code` varchar(16) NOT NULL,
  `continent_name` varchar(64) NOT NULL,
  `country_iso_code` varchar(16) DEFAULT NULL,
  `country_name` varchar(128) DEFAULT NULL,
  `subdivision_1_iso_code` varchar(16) DEFAULT NULL,
  `subdivision_1_name` varchar(64) DEFAULT NULL,
  `subdivision_2_iso_code` varchar(16) DEFAULT NULL,
  `subdivision_2_name` varchar(64) DEFAULT NULL,
  `city_name` varchar(128) DEFAULT NULL,
  `metro_code` varchar(64) DEFAULT NULL,
  `time_zone` varchar(128) DEFAULT NULL,
  `ver_num` int(10) unsigned NOT NULL,
  KEY `geoname_id` (`geoname_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Data exporting was unselected.
-- Dumping structure for table geolite2.t_version
DROP TABLE IF EXISTS `t_version`;
CREATE TABLE IF NOT EXISTS `t_version` (
  `ver_num` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `createdate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`ver_num`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- Data exporting was unselected.
/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS IS NULL, 1, @OLD_FOREIGN_KEY_CHECKS) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
