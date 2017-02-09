CREATE TABLE `kfka` (
  `id` bigint NOT NULL,
  `timestamp` bigint NOT NULL,
  `payload` blob NOT NULL,
  `topic` varchar(255) NOT NULL,
  `type` varchar(255) NOT NULL,
  `userId` int,
  PRIMARY KEY (`id`)
);