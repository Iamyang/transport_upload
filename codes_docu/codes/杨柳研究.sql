--odps sql 
--********************************************************************--
--author:禾季
--create time:2019-05-17 16:28:31
--********************************************************************--


--********************************************************************--
--data filter
--********************************************************************--


drop table if exists tmp_bjjw_ic_card_with_row;
set odps.sql.hive.compatible=true;
set odps.sql.udf.java.retain.legacy=false;
set odps.isolation.session.enable=true;
create table tmp_bjjw_ic_card_with_row as
select *
    ,ROW_NUMBER() OVER(PARTITION BY card_id ORDER BY f_tm) AS row_num
from (
    select ds
        ,card_id
        ,f_mode
        ,f_line
        ,f_dir
        ,fst_num
        ,fst_name
        -- /*转换坐标点为 wkt*/
        ,geospatial.ST_AsText(geospatial.ST_Point(f_lon, f_lat)) as fpt_wkt
        -- /*转换时间字符串为日期*/
        ,to_date(f_tm, 'yyyymmddhhmiss') as f_tm
        ,t_mode
        ,t_line
        ,t_dir
        ,tst_num
        ,tst_name
        -- /*转换坐标点为 wkt*/
        ,geospatial.ST_AsText(geospatial.ST_Point(t_lon, t_lat)) as tpt_wkt
        -- /*转换时间字符串为日期*/
        ,to_date(t_tm, 'yyyymmddhhmiss') as t_tm
    from autonavi_traffic_report.bjjw_ic_card
    where ds = '${date}'
        -- /*删去上下车点一样的记录*/
        and not (f_line = t_line and fst_num = tst_num)
        -- /*删去下车时间超过24点的记录*/	
        and not cast(substring(t_tm, 9, 2) as int) > 23
        -- /*删去下车时间早于上车时间的记录*/	
        and not t_tm <= f_tm
) tb
;

set odps.sql.hive.compatible=true;
set odps.sql.udf.java.retain.legacy=false;
set odps.isolation.session.enable=true;
SELECT  COUNT(*)
FROM    (
            SELECT  *
                    -- /*计算（1）下一次上车时间与上一次下车时间间隔，单位：s*/
                    ,DATEDIFF(t_tm, f_tm, 'ss') AS t_gap
                    -- /*计算（2）下一次上车时间与上一次下车直线距离，单位：m*/
                    ,geospatial.ST_DistanceWGS84(
                        geospatial.ST_GeomFromText(fpt_wkt)
                        ,geospatial.ST_GeomFromText(tpt_wkt)
                    ) AS dist
            FROM    tmp_bjjw_ic_card_with_row
            WHERE   ds = '${date}'
        ) tb
WHERE   dist / t_gap > 25
-- and t_tm < f_tm
;


drop table if exists tmp_bjjw_ic_card_dirty;
set odps.sql.hive.compatible=true;
set odps.sql.udf.java.retain.legacy=false;
set odps.isolation.session.enable=true;
CREATE TABLE tmp_bjjw_ic_card_dirty AS
SELECT  ds
        ,card_id1
        ,row_num1
        ,card_id2
        ,row_num2
FROM    (
            SELECT  ds
                    ,card_id1
                    ,row_num1
                    ,card_id2
                    ,row_num2
                    ,t_gap
                    ,dist
                    -- /*计算（3）下一次上车时间与上一次下车速度，单位：m/s*/
                    ,CASE    WHEN (t_gap > 0) THEN dist / t_gap
                             WHEN (t_gap <= 0 ) THEN 99 
                     END AS speed
            FROM    (
                        SELECT  t1.ds
                                ,t1.card_id as card_id1
                                ,t1.row_num as row_num1
                                ,t2.card_id as card_id2
                                ,t2.row_num as row_num2
                                -- /*计算（1）下一次上车时间与上一次下车时间间隔，单位：s*/
                                ,DATEDIFF(t2.f_tm, t1.t_tm, 'ss') AS t_gap
                                -- /*计算（2）下一次上车时间与上一次下车直线距离，单位：m*/
                                ,geospatial.ST_DistanceWGS84(
                                    geospatial.ST_GeomFromText(t2.fpt_wkt)
                                    ,geospatial.ST_GeomFromText(t1.tpt_wkt)
                                ) AS dist
                        FROM    tmp_bjjw_ic_card_with_row AS t1 LEFT
                        JOIN    tmp_bjjw_ic_card_with_row AS t2
                        ON      ( t1.card_id = t2.card_id AND t1.row_num = t2.row_num - 1 )
                        WHERE   t1.ds = '${date}'
                        AND     t2.ds = '${date}'
                    ) tb
        ) filter
-- 过滤：（1）下一次上车时间与上一次下车时间间隔小于1秒 （2）下一次上车时间与上一次下车时间间隔小于30分钟且速度大于60km/h
WHERE   t_gap < 1
OR      (t_gap < 30 * 60 AND dist / t_gap > 15)
;

drop table if exists tmp_bjjw_ic_card_clean;
CREATE TABLE tmp_bjjw_ic_card_clean AS
SELECT  org.*
        ,dirty.row_num AS dirty_row
FROM    tmp_bjjw_ic_card_with_row AS org LEFT
JOIN    (
            SELECT  DISTINCT card_id1 AS card_id
                    ,row_num1 AS row_num
            FROM    tmp_bjjw_ic_card_dirty
            WHERE   ds = '${date}'
            UNION
            SELECT  DISTINCT card_id2 AS card_id
                    ,row_num2 AS row_num
            FROM    tmp_bjjw_ic_card_dirty
            WHERE   ds = '${date}'
        ) AS dirty
ON      ( org.card_id = dirty.card_id AND org.row_num = dirty.row_num )
WHERE   org.ds = '${date}'
;


select count(*)
from tmp_bjjw_ic_card_clean
where dirty_row is not null;

select *
from tmp_bjjw_ic_card_clean
limit 1000;

drop table if exists tmp_bjjw_ic_card_t_interval;
set odps.sql.hive.compatible=true;
set odps.sql.udf.java.retain.legacy=false;
set odps.isolation.session.enable=true;
create table tmp_bjjw_ic_card_t_interval as
SELECT  t1.ds
        ,t1.card_id
        ,t1.row_num as row_num1
        ,t2.row_num as row_num2
        -- /*计算（1）下一次上车时间与上一次下车时间间隔，单位：s*/
        ,DATEDIFF(t2.f_tm, t1.t_tm, 'ss') AS t_gap
        -- /*计算（2）下一次上车时间与上一次下车直线距离，单位：m*/
        ,geospatial.ST_DistanceWGS84(
            geospatial.ST_GeomFromText(t2.fpt_wkt)
            ,geospatial.ST_GeomFromText(t1.tpt_wkt)
        ) AS dist
FROM    (select * from tmp_bjjw_ic_card_clean where dirty_row is null) AS t1 LEFT
JOIN    (select * from tmp_bjjw_ic_card_clean where dirty_row is null) AS t2
ON      ( t1.card_id = t2.card_id AND t1.row_num = t2.row_num - 1 )
WHERE   t1.ds = '${date}'
AND     t2.ds = '${date}'
;

select *
from tmp_bjjw_ic_card_t_interval
limit 100;

SELECT  t_gap
        ,COUNT(*) AS cnt
FROM    (
            SELECT  floor(t_gap / 300.0 ) * 5 AS t_gap    -- 5min group
            FROM    tmp_bjjw_ic_card_t_interval
        ) tb
GROUP BY t_gap
order by t_gap asc
;


SELECT  dist
        ,COUNT(*) AS cnt
FROM    (
            SELECT  floor(dist / 50.0 ) * 50 AS dist    -- 50 meters group
            FROM    tmp_bjjw_ic_card_t_interval
            where   t_gap < 30*60
        ) tb
GROUP BY dist
order by dist asc
;
