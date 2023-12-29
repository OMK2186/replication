CREATE TABLE `airbyte_txp` (
  `id` int NOT NULL AUTO_INCREMENT,
  `mid` varchar(20) DEFAULT NULL,
  `orderid` varchar(50) NOT NULL,
  `txnamount` double NOT NULL,
  `statecode` tinyint unsigned NOT NULL,
  `createdat` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`))
