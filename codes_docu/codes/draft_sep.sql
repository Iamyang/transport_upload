--9.23------------
DROP TABLE if EXISTS flow_pt;
CREATE TABLE flow_pt AS(
    with tb AS(
        SELECT *
                ,extract(epoch from t_tm::time-f_tm::time) AS dura
                ,ST_Distance(f_position,t_position) AS dist 
        FROM    link_20190301
        WHERE f_mode='GJ' AND t_mode='GJ'
    )
    select  ST_Makeline(f_position,t_position) as geom
            ,fst_name
            ,tst_name
			,count(*) as magnitude
            ,percentile_disc(0.5) within group (order by dura) AS dura
            ,percentile_disc(0.5) within group (order by dist/dura) AS speed 
            ,percentile_disc(0.7) within group (order by extract(epoch from t_tm::time-f_tm::time)) FILTER (WHERE f_tm::time>'06:00:00'::time AND f_tm::time<='09:00:00'::time)
             AS mor_dura
            ,percentile_disc(0.7) within group (order by extract(epoch from t_tm::time-f_tm::time)) FILTER (WHERE f_tm::time>'17:00:00'::time AND f_tm::time<='21:00:00'::time)
             AS eve_dura
            ,percentile_disc(0.7) within group (order by extract(epoch from t_tm::time-f_tm::time)) FILTER (WHERE f_tm::time>'09:00:00'::time AND f_tm::time<='17:00:00'::time)
             AS noo_dura
	from tb
	group by f_position,t_position,fst_name,tst_name
);
DROP TABLE if EXISTS flow_subway;
CREATE TABLE flow_subway AS(
    with tb AS(
        SELECT *
                ,extract(epoch from t_tm::time-f_tm::time) AS dura
                ,ST_Distance(f_position,t_position) AS dist 
        FROM    link_20190301
        WHERE f_mode='DT' AND t_mode='DT'
    )
    select  ST_Makeline(f_position,t_position) as geom
            ,fst_name
            ,tst_name
			,count(*) as magnitude
            ,percentile_disc(0.5) within group (order by dura) AS dura
            ,percentile_disc(0.5) within group (order by dist/dura) AS speed 
            ,percentile_disc(0.7) within group (order by extract(epoch from t_tm::time-f_tm::time)) FILTER (WHERE f_tm::time>'06:00:00'::time AND f_tm::time<='09:00:00'::time)
             AS mor_dura
            ,percentile_disc(0.7) within group (order by extract(epoch from t_tm::time-f_tm::time)) FILTER (WHERE f_tm::time>'17:00:00'::time AND f_tm::time<='21:00:00'::time)
             AS eve_dura
            ,percentile_disc(0.7) within group (order by extract(epoch from t_tm::time-f_tm::time)) FILTER (WHERE f_tm::time>'09:00:00'::time AND f_tm::time<='17:00:00'::time)
             AS noo_dura
	from tb
	group by f_position,t_position,fst_name,tst_name
);
DROP TABLE if EXISTS flow_pt_subway;
CREATE TABLE flow_pt_subway AS(
    with tb AS(
        SELECT *
                ,extract(epoch from t_tm::time-f_tm::time) AS dura
                ,ST_Distance(f_position,t_position) AS dist 
        FROM    link_20190301
        WHERE (f_mode='DT' AND t_mode='GJ') OR (f_mode='GJ' AND t_mode='DT')
    )
    select  ST_Makeline(f_position,t_position) as geom
            ,fst_name
            ,tst_name
			,count(*) as magnitude
            ,percentile_disc(0.5) within group (order by dura) AS dura 
            ,percentile_disc(0.5) within group (order by dist/dura) AS speed 
            ,percentile_disc(0.7) within group (order by extract(epoch from t_tm::time-f_tm::time)) FILTER (WHERE f_tm::time>'06:00:00'::time AND f_tm::time<='09:00:00'::time)
             AS mor_dura
            ,percentile_disc(0.7) within group (order by extract(epoch from t_tm::time-f_tm::time)) FILTER (WHERE f_tm::time>'17:00:00'::time AND f_tm::time<='21:00:00'::time)
             AS eve_dura
            ,percentile_disc(0.7) within group (order by extract(epoch from t_tm::time-f_tm::time)) FILTER (WHERE f_tm::time>'09:00:00'::time AND f_tm::time<='17:00:00'::time)
             AS noo_dura
	from tb
	group by f_position,t_position,fst_name,tst_name
);
--9.22------------
drop table if exists inter_grid_aggr;
create table inter_grid_aggr as(
	select	o_grid
			,d_grid
			,min(mor_dura+(dist_o+dist_d)/1) as mor_dura
			,min(eve_dura+(dist_o+dist_d)/1) as eve_dura
			,min(noo_dura+(dist_o+dist_d)/1) as noo_dura
			,sum(magnitude) as magnitude
	from inter_grid
	group by o_grid,d_grid
)

DROP TABLE if EXISTS hot_inter_grid;
CREATE TABLE hot_inter_grid AS(
    with hot_inter as(
        select *
        from inter_grid_aggr
        where magnitude>=1000
    ),
    drive as(
        select  ST_Transform(ST_GeomFromText('POINT('||o_lon||' '||o_lat||')',4326),32650)::geometry(Point,32650) as o_center
                ,ST_Transform(ST_GeomFromText('POINT('||d_lon||' '||d_lat||')',4326),32650)::geometry(Point,32650) as d_center
                ,driving_dist
        from drive_dist
    )
    select  ST_Makeline(o_center,d_center) as geom
            ,magnitude
            ,driving_dist/least(mor_dura,eve_dura,noo_dura) as max_speed
            ,driving_dist/mor_dura as mor_speed
            ,driving_dist/eve_dura as eve_speed
            ,driving_dist/noo_dura as noo_speed
    FROM    hot_inter AS tb 
    JOIN    drive AS tb1
    ON  ST_Distance(ST_Centroid(tb.o_grid),o_center)<1
    AND ST_Distance(ST_Centroid(tb.d_grid),d_center)<1
)

--9.8-------------
drop table if exists inter_sta_link;
create table inter_sta_link as
(
	select	f_position AS f_sta
            ,t_position AS t_sta
            ,fst_name
            ,tst_name
            ,f_line
            ,t_line
			,count(*) as magnitude
            ,percentile_disc(0.7) within group (order by extract(epoch from t_tm::time-f_tm::time)) FILTER (WHERE f_tm::time>'06:00:00'::time AND f_tm::time<='09:00:00'::time)
             AS mor_dura
            ,percentile_disc(0.7) within group (order by extract(epoch from t_tm::time-f_tm::time)) FILTER (WHERE f_tm::time>'17:00:00'::time AND f_tm::time<='21:00:00'::time)
             AS eve_dura
            ,percentile_disc(0.7) within group (order by extract(epoch from t_tm::time-f_tm::time)) FILTER (WHERE f_tm::time>'09:00:00'::time AND f_tm::time<='17:00:00'::time)
             AS noo_dura
	from link_20190301 as tb
	group by f_position,t_position,fst_name,tst_name,f_line,t_line
);

drop table if exists grid_cell;
 create table grid_cell
 as (
         select	st_translate(a1.first_square, x_series, y_series)::geometry(Polygon,32650) as grid
                ,st_translate(a1.first_point, x_series, y_series)::geometry(Point,32650) as center_point
                ,ROW_NUMBER() OVER() AS gid
         from	generate_series(0, 500*314, 500) as x_series, 
                 generate_series(0, 500*335, 500) as y_series, 
                 ( 
                         select st_geomFromText('POLYGON((354801 4330519,355301 4330519,355301 4331019,354801 4331019,354801 4330519))',32650)::geometry(Polygon,32650) as first_square
                                 ,st_geomFromText('POINT(355051 4330769)',32650)::geometry(Point,32650) as first_point
                 ) as a1
 );

DROP TABLE IF EXISTS grid_sta;
CREATE TABLE grid_sta AS(
    SELECT  tb.gid
            ,tb1.sta_id
            ,tb.grid
            ,tb1.position AS station
    FROM    grid_cell AS tb
    JOIN    station_20190301 AS tb1
    ON   ST_Within(tb1.position,tb.grid)
);
DROP TABLE IF EXISTS inter_grid;
CREATE TABLE inter_grid AS(
    SELECT  tb.grid AS o_grid
            ,tb2.grid AS d_grid
            ,tb1.f_sta
            ,tb1.t_sta
            ,min(tb1.mor_dura) AS mor_dura
            ,min(tb1.eve_dura) AS eve_dura
            ,min(tb1.noo_dura) AS noo_dura
            ,sum(tb1.magnitude) AS magnitude
            ,ST_Distance(ST_Centroid(tb.grid),tb1.f_sta) AS dist_o
            ,ST_Distance(ST_Centroid(tb2.grid),tb1.t_sta) AS dist_d
    FROM    grid_sta AS tb 
    JOIN    inter_sta_link AS tb1
    ON  ST_Equals(tb.station,tb1.f_sta)
    JOIN    grid_sta AS tb2
    ON ST_Equals(tb2.station,tb1.t_sta)
    GROUP BY tb.grid,tb2.grid,tb1.f_sta,tb1.t_sta
);

SELECT  ST_Astext(ST_Transform(ST_Centroid(o_grid),4326)::geography AS o_grid
        ,ST_Astext(ST_Transform(ST_Centroid(d_grid),4326)::geography AS d_grid
        ,ST_Astext(ST_Transform(ST_Centroid(f_sta),4326)::geography AS f_sta
        ,ST_Astext(ST_Transform(ST_Centroid(t_sta),4326)::geography AS t_sta
        ,ROW_NUMBER() OVER(PARTITION BY o_grid,d_grid ORDER BY magnitude)
FROM    inter_grid



--9.6-------------
--about 3 min
DROP TABLE IF EXISTS split_trsform;
CREATE TABLE split_trsform AS(
    SELECT	card_id
            ,f_mode
            ,f_line
            ,f_dir
            ,cast (fst_num as integer) AS fst_num
            ,fst_name
            ,substring(t_mode,'..$') AS t_mode
            ,t_line
            ,t_dir
            ,cast (tst_num as integer) AS tst_num
            ,tst_name
            ,to_timestamp(f_tm,'YYYYMMDDHH24MISS') as f_tm
            ,to_timestamp(t_tm,'YYYYMMDDHH24MISS') as t_tm
            ,ST_Transform(ST_GeomFromText('POINT('||f_lon||' '||f_lat||')',4326),32650)::geometry(Point,32650) as f_position
            ,ST_Transform(ST_GeomFromText('POINT('||t_lon||' '||t_lat||')',4326),32650)::geometry(Point,32650) as t_position
    FROM	split_20190301
)

--station
drop table if exists station_20190301;
create table station_20190301
as(
    WITH tb AS(
        select distinct mode,line,dir,st_num,name,position
        from	(
                    select distinct f_mode as mode
                                ,f_line as line
                                ,f_dir as dir
                                ,fst_num as st_num
                                ,fst_name as name
                                ,f_position as position
                    from split_trsform
                    union
                    select	distinct t_mode as mode
                                ,t_line as line
                                ,t_dir as dir
                                ,tst_num as st_num
                                ,tst_name as name
                                ,t_position as position
                    from split_trsform
                )a
        order by mode,line,dir,st_num,name,position
    )
    SELECT  ROW_NUMBER() OVER() AS sta_id
            ,*
    FROM    tb  
        
);


drop table if exists inter_sta_split;
create table inter_sta_split as
(
	select	f_position
            ,t_position
			,count(*) as cnt
	from split_trsform as tb
    WHERE f_position!=t_position
	group by f_position,t_position
);

drop table if exists inter_sta_cle;
create table inter_sta_cle as
(
	select	f_position
            ,t_position
			,count(*) as cnt
	from cleaned_20190301 as tb
	group by f_position,t_position
);

