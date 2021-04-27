---
-- #%L
-- kfka
-- %%
-- Copyright (C) 2017 Morten Haraldsen (ethlo)
-- %%
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
--      http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
-- #L%
---
CREATE TABLE `kfka` (
  `id` bigint AUTO_INCREMENT NOT NULL,
  `message_id` char(12) NOT NULL,
  `timestamp` bigint NOT NULL,
  `payload` blob NOT NULL,
  `topic` varchar(255) NOT NULL,
  `type` varchar(255) NOT NULL,
  `userId` int,
  PRIMARY KEY (`id`)
);
