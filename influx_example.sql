-- Default retention to 7 days
ALTER RETENTION POLICY "autogen" ON "victron" DURATION 7d SHARD DURATION 1d DEFAULT
-- https://www.neteye-blog.com/2019/12/downsampling-performance-data-in-influxdb/
-- 7d
INSERT INTO inf rp_config,idx=1 rp="autogen",start=0i,end=604800000i -9223372036854775806;
-- 30d
INSERT INTO inf rp_config,idx=2 rp="30d",start=604800000i,end=2592000000i -9223372036854775806;
-- 365d
INSERT INTO inf rp_config,idx=3 rp="365d",start=2592000000i,end=31536000000i -9223372036854775806 ;
-- inf
INSERT INTO inf rp_config,idx=4 rp="inf",start=31536000000i,end=9999999999999i -9223372036854775806 ; 
-- Variable name: rp
-- Type: Query
-- Query: SELECT rp FROM forever.rp_config WHERE $__to - $__from > "start" AND $__to - $__from <= "end"

-- Default Retention policy: 7d

-- DROP RETENTION POLICY "30d" ON "victron";
CREATE RETENTION POLICY "30d" ON "victron" DURATION 30d REPLICATION 1
CREATE RETENTION POLICY "365d" ON "victron" DURATION 365d REPLICATION 1
CREATE RETENTION POLICY "inf" ON "victron" DURATION INF REPLICATION 1

DROP CONTINUOUS QUERY "cq_30d" ON "victron";
CREATE CONTINUOUS QUERY "cq_30d" ON "victron"
BEGIN
  SELECT mean(value) AS "value" INTO "victron"."30d".:MEASUREMENT FROM "autogen"./.*/ GROUP BY time(60s), instanceNumber, path, portalId
END;

DROP CONTINUOUS QUERY "cq_365d" ON "victron";
CREATE CONTINUOUS QUERY "cq_365d" ON "victron"
BEGIN
  SELECT mean(value) AS "value" INTO "victron"."365d".:MEASUREMENT FROM "30d"./.*/ GROUP BY time(5m), instanceNumber, path, portalId
END;

DROP CONTINUOUS QUERY "cq_inf" ON "victron";
CREATE CONTINUOUS QUERY "cq_inf" ON "victron"
BEGIN
  SELECT mean(value) AS "value" INTO "victron"."inf".:MEASUREMENT FROM "365d"./.*/ GROUP BY time(10m), instanceNumber, path, portalId
END;
