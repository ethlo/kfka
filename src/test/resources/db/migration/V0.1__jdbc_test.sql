CREATE TABLE `kfka` (
  `id` bigint(20) NOT NULL,
  `timestamp` timestamp NOT NULL,
  `message` blob NOT NULL,
  `topic` varchar(255) NOT NULL,
  `type` varchar(255) NOT NULL,
  `organization_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  PRIMARY KEY (`id`)
);