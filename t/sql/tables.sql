CREATE TABLE `k1` (
  `id` int(11) NOT NULL,
  `name` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1
PARTITION BY KEY ()
PARTITIONS 2;

CREATE TABLE `employees` (
  `id` int(11) NOT NULL,
  `fname` varchar(30) DEFAULT NULL,
  `lname` varchar(30) DEFAULT NULL,
  `hired` date NOT NULL DEFAULT '1970-01-01',
  `separated` date NOT NULL DEFAULT '9999-12-31',
  `job_code` int(11) DEFAULT NULL,
  `store_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1
PARTITION BY HASH (YEAR(hired))
PARTITIONS 4;

CREATE TABLE `employees2` (
  `id` int(11) NOT NULL,
  `fname` varchar(30) DEFAULT NULL,
  `lname` varchar(30) DEFAULT NULL,
  `hired` date NOT NULL DEFAULT '1970-01-01',
  `separated` date NOT NULL DEFAULT '9999-12-31',
  `job_code` int(11) DEFAULT NULL,
  `store_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1
PARTITION BY LIST (store_id)
(PARTITION pNorth VALUES IN (3,5,6,9,17) COMMENT = 'north' ENGINE = InnoDB,
 PARTITION pEast VALUES IN (1,2,10,11,19,20) COMMENT = 'east' ENGINE = InnoDB,
 PARTITION pWest VALUES IN (4,12,13,14,18) COMMENT = 'west' ENGINE = InnoDB,
 PARTITION pCentral VALUES IN (7,8,15,16) COMMENT = 'central' ENGINE = InnoDB);

CREATE TABLE `employees3` (
  `id` int(11) NOT NULL,
  `fname` varchar(30) DEFAULT NULL,
  `lname` varchar(30) DEFAULT NULL,
  `hired` date NOT NULL DEFAULT '1970-01-01',
  `separated` date NOT NULL DEFAULT '9999-12-31',
  `job_code` int(11) NOT NULL,
  `store_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1
PARTITION BY RANGE (store_id)
(PARTITION p0 VALUES LESS THAN (6) ENGINE = InnoDB,
 PARTITION p1 VALUES LESS THAN (11) ENGINE = InnoDB,
 PARTITION p2 VALUES LESS THAN (16) ENGINE = InnoDB,
 PARTITION p3 VALUES LESS THAN (21) ENGINE = InnoDB);

CREATE TABLE `activities` (
  `id` bigint(20) unsigned not null,
  `title` varchar(32) not null,
  `created_on` datetime not null,
  primary key (`id`, `created_on`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
PARTITION BY RANGE (TO_DAYS(created_on)) (
  PARTITION p20100914 VALUES LESS THAN 
    (TO_DAYS('2010-09-15 00:00:00')) ENGINE = InnoDB COMMENT = '2010-09-14',
  PARTITION p20100915 VALUES LESS THAN 
    (TO_DAYS('2010-09-16 00:00:00')) ENGINE = InnoDB COMMENT = '2010-09-15'
);
