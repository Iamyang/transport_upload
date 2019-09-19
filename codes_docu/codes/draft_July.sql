--7.31---------------
CREATE OR REPLACE FUNCTION equal_point(
   dura_ar interval[],
   index_ar integer[]
) 
RETURNS integer AS $$  
DECLARE  
    equal_idx integer;
    previous INTERVAL :='24 hours' ;
    currt INTERVAL :='24 hours';
BEGIN
    FOR i IN 1..array_length(dura_ar,1) LOOP
        currt :=(SELECT SUM(s) FROM UNNEST(arr[1:1]) s);
        IF (currt>previous) THEN
                equal_idx:=i-1;
                EXIT;
        ELSE
                previous:=currt;
        END IF;
   END LOOP;
   RETURN index_ar[equal_idx];  
END;  
$$ LANGUAGE plpgsql;



--7.25---------------
alter table grid_5ring 
 add column if not exists gid integer;
update grid_5ring
set     gid=cast((st_x(center_point)-432551)/500+((st_y(center_point)-4401269)/500)*58 as integer)
;
--7.22---------------
DROP TABLE IF EXISTS access_14_inter;
CREATE TABLE access_14_inter AS
(
        SELECT  inter.*
                ,(ac.drive_dist+0.001)/ac.transit_dura AS speed
        FROM    access_14 AS ac
        LEFT JOIN    inter_grid_5ring AS inter
        ON      f_grid=o_geom
        AND     t_grid=d_geom
);
--accessibility
DROP TABLE IF EXISTS access_14;
CREATE TABLE access_14 AS
(
    SELECT  tb.*
        ,ST_Azimuth(o_geom,d_geom) AS azimuth
        ,ceil(ST_Azimuth(o_geom,d_geom)/(PI()/4)) AS azi_group
         
    FROM    (
                SELECT  *
                        ,ST_Transform(ST_GeomFromText('POINT('||o_lon||' '||o_lat||')',4326),32650)::geometry(Point,32650) AS o_geom
                        ,ST_Transform(ST_GeomFromText('POINT('||d_lon||' '||d_lat||')',4326),32650)::geometry(Point,32650) AS d_geom
                FROM    dura_dist_14
                WHERE   drive_dist>0 AND transit_dist>0        
                UNION ALL
                SELECT  rid 
                        ,transit_dura
                        ,transit_dist
                        ,walking_distance
                        ,n_transfer
                        ,drive_dura
                        ,drive_dist
                        ,d_lon AS o_lon
                        ,d_lat AS o_lat
                        ,o_lon AS d_lon
                        ,o_lat AS d_lat
                        ,ST_Transform(ST_GeomFromText('POINT('||o_lon||' '||o_lat||')',4326),32650)::geometry(Point,32650) AS d_geom
                        ,ST_Transform(ST_GeomFromText('POINT('||d_lon||' '||d_lat||')',4326),32650)::geometry(Point,32650) AS o_geom
                FROM    dura_dist_14
                WHERE   drive_dist>0 AND transit_dist>0
            )tb 
)
;
--7.20---------------
drop table if exists od_46;
  create table od_46
  as		(
  			select t1.center_point as p1
  				,t2.center_point as p2
 				,st_distance(t1.center_point,t2.center_point) as distance
  			from	grid_5ring as t1
 			join	grid_5ring as t2
 			on	t1.center_point<t2.center_point
 			where	st_distance(t1.center_point,t2.center_point)>=4000
                                AND st_distance(t1.center_point,t2.center_point)<6000
  				
  		)
  ;
copy	(
		select st_astext(st_transform(p1,4326)::geography)
                        ,st_astext(st_transform(p2,4326)::geography)
		from	od_14
	)
to 'E:/RESEARCH/Transportation/Data/od_46.txt'
;
--7.12---------------
drop table if exists inter_sta_5ring;
create table inter_sta_5ring as
(
        SELECT  f_position
                ,t_position
                ,cnt 
        FROM    (
                SELECT  ST_Transform(ST_GeomFromText(st_astext
                        (f_position),4326),32650)::geometry(Point,32650) AS f_position
                        ,ST_Transform(ST_GeomFromText(st_astext
                        (t_position),4326),32650)::geometry(Point,32650) AS t_position
                        ,cnt 
                FROM    interation_sta
        )tb 
        JOIN    (
                SELECT  ST_Union(geom) AS geom
                FROM    grid_5ring
        )gr    
        ON      ST_Contains(gr.geom,tb.f_position)
        AND     ST_Contains(gr.geom,tb.t_position)
)

drop table if exists inter_grid_5ring;
create table inter_grid_5ring as
(
        SELECT  gr1.geom as f_grid
                ,gr2.geom as t_grid
                ,sum(sta.cnt) AS cnt
        FROM    grid_5ring AS gr1
        LEFT JOIN  inter_sta_5ring AS sta  
        ON      ST_Contains(gr1.geom,sta.f_position)
        RIGHT JOIN grid_5ring AS gr2
        ON      ST_Contains(gr2.geom,sta.t_position)
        GROUP BY gr1.geom,gr2.geom

)

copy	(
		select  *
		from	inter_grid_5ring
	)
to 'D:/amap_transport/data/inter_grid_5ring.csv'(format csv, delimiter ',')
;

--7.9-----------------
drop table if exists station_unique;
create table station_unique as
(
        SELECT  DISTINCT ST_GeomFromText(st_astext(position),4326)::geometry(Point,4326) AS geom 
        FROM    station_20190301
);
select FreeGIS_Coordinate_Transform('public','station_unique','GCJ2WGS');


drop table if exists interation_sta;
create table interation_sta as
(
	select	distinct f_position,t_position
			,count(*) as cnt
	from link_20190301 as tb
	group by f_position,t_position
);

drop table if exists grid_cell;
 create table grid_cell
 as (
         select	st_translate(a1.first_square, x_series, y_series)::geometry(Polygon,32650) as grid
                 ,st_translate(a1.first_point, x_series, y_series)::geometry(Point,32650) as center_point
         from	generate_series(0, 500*109, 500) as x_series, 
                 generate_series(0, 500*117, 500) as y_series, 
                 ( 
                         select st_geomFromText('POLYGON((427301 4390019,427301 4390519,427801 4390519,427801 4390019,427301 4390019))',32650)::geometry(Polygon,32650) as first_square
                                 ,st_geomFromText('POINT(427551 4390269)',32650)::geometry(Point,32650) as first_point
                 ) as a1
 );


--7.7-----------------
drop table if exists core_area_poly;
create table core_area_poly
as			(
				select st_polygonfromtext('POLYGON((116.782512 39.942293,116.781832 39.937682,116.779227 39.935356,116.742085 39.923493,116.740699 39.918116,116.74266 39.90859,116.738404 39.888296,116.739754 39.885002,116.745435 39.880081,116.749063 39.865424,116.755008 39.860195,116.777674 39.85923,116.781506 39.856864,116.782346 39.851378,116.778524 39.846371,116.778046 39.841385,116.775261 39.837794,116.769665 39.837586,116.743784 39.851823,116.735249 39.854141,116.730239 39.853056,116.727835 39.8475,116.72405 39.844792,116.719469 39.845603,116.715224 39.849371,116.711525 39.849274,116.706354 39.842228,116.698411 39.840667,116.69191 39.833,116.681961 39.831251,116.664079 39.816197,116.66055 39.809953,116.65384 39.808412,116.641152 39.79878,116.637632 39.789439,116.630143 39.786523,116.628066 39.784148,116.617761 39.764504,116.615485 39.75531,116.612432 39.753653,116.595782 39.752078,116.592708 39.750294,116.591573 39.746836,116.594531 39.738076,116.591141 39.733579,116.579918 39.734451,116.577037 39.732648,116.574389 39.728418,116.56988 39.726937,116.566554 39.72832,116.561646 39.733392,116.546861 39.731789,116.540772 39.734151,116.536791 39.738295,116.533729 39.738861,116.523328 39.735239,116.504118 39.732639,116.487784 39.725818,116.47236 39.722425,116.466543 39.718562,116.458915 39.718715,116.454628 39.711738,116.450526 39.709655,116.446154 39.711089,116.442629 39.716119,116.43936 39.717584,116.417552 39.716563,116.412334 39.711001,116.409117 39.709642,116.405689 39.710304,116.398951 39.715839,116.38832 39.715909,116.383182 39.711261,116.366359 39.703065,116.336649 39.695552,116.330046 39.678976,116.336198 39.67054,116.336075 39.666072,116.3273 39.658965,116.323972 39.658012,116.319774 39.659806,116.315672 39.665787,116.321011 39.679294,116.316623 39.686295,116.308569 39.691001,116.306982 39.69536,116.308174 39.698672,116.314702 39.704024,116.315978 39.714396,116.323177 39.718369,116.324239 39.723073,116.322766 39.727028,116.317118 39.733068,116.315384 39.740061,116.308923 39.743627,116.303187 39.751151,116.299637 39.768716,116.295679 39.770919,116.284437 39.768308,116.281551 39.766113,116.279011 39.761135,116.275798 39.759674,116.27232 39.760272,116.267601 39.764195,116.259456 39.766428,116.257906 39.769515,116.258315 39.772946,116.264454 39.779337,116.265673 39.782767,116.261246 39.796229,116.259572 39.807289,116.245025 39.824989,116.233794 39.829501,116.22402 39.83864,116.219887 39.839199,116.210466 39.837051,116.2026 39.828192,116.192996 39.826944,116.188607 39.828562,116.184621 39.839192,116.185874 39.842316,116.190279 39.846558,116.19071 39.852784,116.194351 39.858255,116.193706 39.862026,116.188033 39.866559,116.187058 39.873267,116.185043 39.876146,116.179914 39.878046,116.1688 39.876633,116.160429 39.883413,116.159154 39.886691,116.159936 39.89012,116.162505 39.892522,116.169635 39.89577,116.180259 39.897602,116.181892 39.903071,116.176266 39.908927,116.174227 39.916006,116.171791 39.918531,116.154254 39.9193,116.151204 39.920952,116.149577 39.924016,116.149164 39.932928,116.14993 39.936351,116.153587 39.939179,116.164265 39.937832,116.17092 39.940689,116.177649 39.939433,116.192269 39.940908,116.195049 39.942541,116.198675 39.948048,116.206782 39.949359,116.209703 39.95139,116.215869 39.983557,116.213738 39.986789,116.204187 39.988662,116.200616 39.991647,116.200253 39.996287,116.206111 40.004257,116.211794 40.005103,116.21669 40.000138,116.220527 39.999299,116.248018 40.007295,116.258166 40.008195,116.261256 40.010841,116.261874 40.015774,116.258199 40.024811,116.254831 40.026405,116.248695 40.023955,116.2441 40.024823,116.239814 40.029437,116.238956 40.036865,116.231365 40.041142,116.229604 40.045537,116.233215 40.051557,116.242064 40.056475,116.244083 40.059531,116.243287 40.063496,116.236426 40.069348,116.235903 40.072701,116.237282 40.075802,116.257792 40.083014,116.26073 40.085037,116.26201 40.088705,116.259813 40.092548,116.239694 40.107527,116.236526 40.113731,116.230319 40.11721,116.228961 40.122863,116.237421 40.131064,116.241 40.13086,116.247304 40.126333,116.253654 40.127352,116.255481 40.13044,116.255059 40.13772,116.259491 40.142834,116.263129 40.143431,116.268957 40.141607,116.27552 40.133555,116.276777 40.128044,116.279896 40.124194,116.28108 40.120078,116.301969 40.102823,116.305008 40.103209,116.309697 40.107923,116.313016 40.108714,116.322472 40.103251,116.346474 40.104314,116.349647 40.108438,116.35129 40.116954,116.357967 40.119852,116.371901 40.118308,116.37638 40.113744,116.381339 40.111236,116.384206 40.111759,116.388386 40.115187,116.391752 40.115577,116.40083 40.108431,116.4046 40.109174,116.409627 40.113676,116.414233 40.113212,116.417138 40.109608,116.418674 40.098551,116.42217 40.094783,116.429776 40.091371,116.439656 40.092918,116.448704 40.087807,116.450823 40.084985,116.450814 40.079535,116.446195 40.073543,116.447396 40.066481,116.450714 40.059805,116.447188 40.052802,116.447868 40.039416,116.452935 40.037376,116.460746 40.041079,116.46742 40.038703,116.474133 40.042043,116.477252 40.047142,116.477146 40.052525,116.474258 40.058956,116.476992 40.06552,116.476436 40.08287,116.477424 40.086387,116.485167 40.09015,116.487872 40.0968,116.490507 40.099275,116.512963 40.100772,116.517178 40.102745,116.517859 40.11075,116.511783 40.118741,116.51381 40.124185,116.52429 40.126712,116.535162 40.12727,116.537692 40.129034,116.54001 40.134269,116.544005 40.136366,116.574669 40.135736,116.577472 40.138673,116.577578 40.144246,116.580285 40.148053,116.595947 40.148795,116.62889 40.166222,116.637139 40.179759,116.641088 40.181801,116.645358 40.180564,116.649928 40.175863,116.6736 40.175641,116.6775 40.172978,116.677857 40.167176,116.673995 40.162629,116.673537 40.156333,116.668227 40.145931,116.665036 40.142448,116.664619 40.138744,116.673119 40.127449,116.679291 40.122758,116.688467 40.120251,116.690813 40.114934,116.688155 40.109298,116.681453 40.102967,116.670578 40.103071,116.66646 40.099826,116.666671 40.071225,116.671384 40.064677,116.671685 40.061278,116.667281 40.055607,116.660673 40.053037,116.65768 40.049427,116.649526 40.048456,116.646672 40.046029,116.646136 40.043238,116.64979 40.028427,116.648027 40.023216,116.640752 40.021111,116.636895 40.017667,116.632431 40.016588,116.622583 40.017069,116.611927 40.020282,116.609126 40.018921,116.606615 40.015095,116.606053 40.010161,116.612821 40.003494,116.613745 39.993096,116.617611 39.986808,116.61803 39.98323,116.616324 39.980057,116.611652 39.97705,116.592147 39.973425,116.588607 39.971126,116.587842 39.968013,116.589645 39.952886,116.594584 39.940138,116.598638 39.937318,116.612804 39.937381,116.619726 39.932812,116.627015 39.935276,116.633922 39.933837,116.640037 39.937121,116.646583 39.936843,116.650166 39.93866,116.651652 39.941981,116.651638 39.950715,116.655198 39.953988,116.662471 39.953631,116.669475 39.958422,116.696286 39.959106,116.701359 39.964108,116.704532 39.96519,116.708715 39.963747,116.713243 39.957694,116.721025 39.956145,116.726238 39.949139,116.747578 39.949864,116.772161 39.94787,116.779805 39.94594,116.782512 39.942293))',4326)::geography(Polygon,4326) AS geom
				
			)
;


drop table if exists grid_cell;
 create table grid_cell
 as (
         select	st_translate(a1.first_square, x_series, y_series)::geometry(Polygon,32650) as grid
                 ,st_translate(a1.first_point, x_series, y_series)::geometry(Point,32650) as center_point
         from	generate_series(0, 500*109, 500) as x_series, 
                 generate_series(0, 500*117, 500) as y_series, 
                 ( 
                         select st_geomFromText('POLYGON((427301 4390019,427301 4390519,427801 4390519,427801 4390019,427301 4390019))',32650)::geometry(Polygon,32650) as first_square
                                 ,st_geomFromText('POINT(427551 4390269)',32650)::geometry(Point,32650) as first_point
                 ) as a1
 )	
 ;
drop table if exists grid_5ring;
create table grid_5ring
as		(
			select	gc.grid as grid
				,gc.center_point as center_point
			from	grid_cell as gc 
			join	taz_5ring_wgs84 as taz
			on st_contains(taz.transform_geom_utm,gc.center_point)
		)	
; 

drop table if exists od_14;
  create table od_14
  as		(
  			select t1.center_point as p1
  				,t2.center_point as p2
 				,st_distance(t1.center_point,t2.center_point) as distance
  			from	grid_5ring as t1
 			join	grid_5ring as t2
 			on	t1.center_point<t2.center_point
 			where	st_distance(t1.center_point,t2.center_point)>=1000
                                AND st_distance(t1.center_point,t2.center_point)<4000
  				
  		)
  ;
copy	(
		select st_astext(st_transform(p1,4326)::geography)
                        ,st_astext(st_transform(p2,4326)::geography)
		from	od_14
	)
to 'E:/RESEARCH/Transportation/script/amap_transport/data/od_14.txt'
;
--检查异常返回结果
select total_count-filtered_count as normal_count
		,tb.*
from	(
		SELECT	count(*) as total_count
				,count(*) filter(where	drive_dist<0 or transit_dist<0) as filtered_count
				,ST_GeogFromText('SRID=4326;POINT('||o_lon||' '||o_lat||')')::geography AS o_geom
		FROM    dura_dist
		group by ST_GeogFromText('SRID=4326;POINT('||o_lon||' '||o_lat||')')::geography
	)tb
order by total_count-filtered_count
	
--accessibility
DROP TABLE IF EXISTS accessibility;
CREATE TABLE accessibility AS
(
    SELECT  tb.*
        ,ST_Azimuth(o_geom,d_geom) AS azimuth
        ,ceil(ST_Azimuth(o_geom,d_geom)/(PI()/4)) AS azi_group
         
    FROM    (
                SELECT  *
                        ,ST_GeogFromText('SRID=4326;POINT('||o_lon||' '||o_lat||')')::geography(Point,4326) AS o_geom
                        ,ST_GeogFromText('SRID=4326;POINT('||d_lon||' '||d_lat||')')::geography(Point,4326) AS d_geom
                FROM    dura_dist
                WHERE   drive_dist>0 AND transit_dist>0        
                UNION ALL
                SELECT  rid 
                        ,transit_dura
                        ,transit_dist
                        ,walking_distance
                        ,n_transfer
                        ,drive_dura
                        ,drive_dist
                        ,d_lon AS o_lon
                        ,d_lat AS o_lat
                        ,o_lon AS d_lon
                        ,o_lat AS d_lat
                        ,ST_GeogFromText('SRID=4326;POINT('||o_lon||' '||o_lat||')')::geography(Point,4326) AS d_geom
                        ,ST_GeogFromText('SRID=4326;POINT('||d_lon||' '||d_lat||')')::geography(Point,4326) AS o_geom
                FROM    dura_dist
                WHERE   drive_dist>0 AND transit_dist>0
            )tb 
)
;

COPY(
        SELECT  avg((drive_dist+0.001)/transit_dura) AS speed_avg
                ,avg((drive_dist+0.001)/transit_dist) AS rat_dist_avg
                ,avg((drive_dura+0.001)/transit_dura) AS rat_dura_avg
                ,stddev_samp((drive_dist+0.001)/transit_dura) AS speed_std
                ,stddev_samp((drive_dist+0.001)/transit_dist) AS rat_dist_std
                ,stddev_samp((drive_dura+0.001)/transit_dura) AS rat_dura_std
        FROM    accessibility
        GROUP BY o_geom
)
TO 'E:/RESEARCH/Transportation/script/amap_transport/data/acc_avg_std.csv'(format csv, delimiter ',')
;

DROP TABLE IF EXISTS acc_avg_std;
CREATE TABLE acc_avg_std AS
(
        SELECT  speed_avg
                ,rat_dist_avg
                ,rat_dura_avg
                ,speed_std
                ,rat_dist_std
                ,rat_dura_std
                ,grid
        FROM (
                SELECT  avg((drive_dist+0.001)/transit_dura) AS speed_avg
                        ,avg((drive_dist+0.001)/transit_dist) AS rat_dist_avg
                        ,avg((drive_dura+0.001)/transit_dura) AS rat_dura_avg
                        ,stddev_samp((drive_dist+0.001)/transit_dura) AS speed_std
                        ,stddev_samp((drive_dist+0.001)/transit_dist) AS rat_dist_std
                        ,stddev_samp((drive_dura+0.001)/transit_dura) AS rat_dura_std
                        ,st_transform(ST_GeomFromText(st_astext(o_geom),4326),32650)::geometry(Point,32650) AS o_geom
                FROM    accessibility
                GROUP BY o_geom
        )tb
        JOIN    grid_5ring
        ON      st_contains(grid_5ring.grid,tb.o_geom)
        
);

COPY(
        SELECT  (drive_dist+0.001)/transit_dura AS speed
                ,ST_Distance(o_geom,d_geom) AS distance
                ,drive_dist
        FROM    accessibility
        
)
TO 'E:/RESEARCH/Transportation/script/amap_transport/data/acc_dist_dura.csv'(format csv, delimiter ',')
;

--7.4----------------------
COPY (
        SELECT  tb.*
                ,st_astext(position) AS lon_lat
        FROM station_20190301 AS tb
)
TO 'D:/amap_transport/data/station_20190301.csv'(format csv, delimiter ',');
--流距离--
DROP TABLE IF EXISTS fds_sample;
CREATE TABLE fds_sample
AS (
	SELECT	tb.card_id as card_id1
			,tb.row_num AS row_num1
			,tb1.card_id as card_id2
			,tb.row_num AS row_num2
			,tb.fst_name AS fst_name1
			,tb.tst_name AS tst_name1
			,tb1.fst_name AS fst_name2
			,tb1.tst_name AS tst_name2
			,tb.f_position AS f_position1
			,tb.t_position AS t_position1
			,tb1.f_position AS f_position2
			,tb1.t_position AS t_position2
			,sqrt((ST_Distance(tb.f_position,tb1.f_position)^2+ST_Distance(tb.t_position,tb1.t_position)^2)/
			(ST_Distance(tb.t_position,tb.f_position)*ST_Distance(tb1.t_position,tb1.f_position))
			) AS fds
			,sqrt(ST_Distance(tb.f_position,tb1.f_position)^2+ST_Distance(tb.t_position,tb1.t_position)^2)
			 AS fd
	FROM	link_sample1w AS tb 
	JOIN	link_sample1w AS tb1
)
;


--flow--
SELECT  ST_Distance(o_geom_gra,d_geom_gra) AS dist1
        ,ST_Distance(o_geom,d_geom) AS dist2
        ,ST_Azimuth(o_geom_gra,d_geom_gra) AS azimuth1
        ,ST_Azimuth(o_geom,d_geom) AS azimuth2
        ,*
FROM    (
            SELECT  ST_GeogFromText('SRID=4326;POINT('||o_lon||' '||o_lat||')')::geography AS o_geom_gra
                    ,ST_GeogFromText('SRID=4326;POINT('||d_lon||' '||d_lat||')')::geography AS d_geom_gra
                    ,ST_GeomFromText('POINT('||o_lon||' '||o_lat||')',4326)::geometry AS o_geom
                    ,ST_GeomFromText('POINT('||d_lon||' '||d_lat||')',4326)::geometry AS d_geom
            FROM    dura_dist
)tb
LIMIT 100
