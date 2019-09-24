DROP TABLE IF EXISTS ic_wj;
CREATE TABLE ic_wj(
	card_id character varying
	,f_mode character varying
	,f_line character varying
	,f_dir	character varying
	,fst_num character varying
	,fst_name character varying
	,f_lon character varying
	,f_lat character varying
	,f_tm character varying
	,t_mode character varying
	,t_line character varying
	,t_dir	character varying
	,tst_num character varying
	,tst_name character varying
	,t_lon	character varying
	,t_lat	character varying
	,t_tm	character varying
);
COPY	ic_wj
FROM	'E:\RESEARCH\Transportation\Data\wjcase_ic_card.csv' DELIMITER ',';

DROP TABLE IF EXISTS icwj_trsform;
CREATE TABLE icwj_trsform AS(
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
    FROM	ic_wj
);

DROP TABLE IF EXISTS commute_wj;
CREATE TABLE commute_wj AS(
	id character varying 
	,lon_home DOUBLE PRECISION 
	,lat_home DOUBLE PRECISION
	,lon_work DOUBLE PRECISION
	,lat_work DOUBLE PRECISION
	,lon_home_center DOUBLE PRECISION
	,lat_home_center DOUBLE PRECISION
	,lon_work_center DOUBLE PRECISION
	,lat_work_center DOUBLE PRECISION
	,tm_work_lower DOUBLE PRECISION
	,tm_work_upper DOUBLE PRECISION
	,tm_home_lower DOUBLE PRECISION
	,tm_home_upper DOUBLE PRECISION
	,home_wj character varying
	,work_wj character varying
);
COPY	commute_wj
FROM	'E:\RESEARCH\Transportation\Data\wjcase_home_company.csv' DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS com_trsform;
CREATE TABLE com_trsform AS(
	SELECT	id
			,ST_Transform(ST_GeomFromText('POINT('||lon_home||' '||lat_home||')',4326),32650)::geometry(Point,32650) as home_pos
            ,ST_Transform(ST_GeomFromText('POINT('||lon_work||' '||lat_work||')',4326),32650)::geometry(Point,32650) as work_pos
			,ST_Transform(ST_GeomFromText('POINT('||lon_home_center||' '||lat_home_center||')',4326),32650)::geometry(Point,32650) as home_center
            ,ST_Transform(ST_GeomFromText('POINT('||lon_work_center||' '||lat_work_center||')',4326),32650)::geometry(Point,32650) as work_center
			,tm_work_lower
			,tm_home_lower
			,home_wj
			,work_wj
	FROM	commute_wj
);

CREATE TABLE poly_wj AS (
	SELECT ST_Transform(st_polygonfromtext('POLYGON((116.454674 40.018324,116.460524 40.016090,116.468234 40.016072,116.475145 40.015966,116.483915 40.015320,116.487723 40.012820,116.491973 40.008807,116.496931 40.003815,116.500650 40.000870,116.502244 39.998998,116.499498 39.996695,116.493565 39.991824,116.479924 39.979595,116.472127 39.972327,116.469114 39.969134,116.464241 39.973324,116.456619 39.978852,116.448728 39.983845,116.444382 39.985541,116.440035 39.987059,116.443496 39.991320,116.447932 39.998247,116.450415 40.003043,116.452811 40.011661,116.454674 40.018324))',4326),32650)::geometry(POLYGON,32650)
	AS geom
);