--7.1--------------------------

DROP TABLE IF EXISTS fds_20190301;
CREATE TABLE fds_20190301 
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
	FROM	link_20190301 AS tb 
	JOIN	link_20190301 AS tb1
	ON		sqrt((ST_Distance(tb.f_position,tb1.f_position)^2+ST_Distance(tb.t_position,tb1.t_position)^2)/
			(ST_Distance(tb.t_position,tb.f_position)*ST_Distance(tb1.t_position,tb1.f_position))
			)<1
)
;
/*创建临时表，仅存储前后两次换乘出行记录的行号。判断换乘规则：时间间隔小于30分钟，距离间隔小于300m,方向角之差小于170度*/ 
-- drop table if exists transfer_20190301_;
-- create table transfer_20190301_
-- as	(
-- 		select	tb.card_id
-- 				,tb.t_mode
-- 				,tb.t_line
-- 				,tb.t_dir
-- 				,tb.tst_num
-- 				,tb.tst_name
-- 				,tb.t_lon
-- 				,tb.t_lat
-- 				,tb.t_tm
-- 				,tb1.f_mode
-- 				,tb1.f_line
-- 				,tb1.f_dir
-- 				,tb1.fst_num
-- 				,tb1.fst_name
-- 				,tb1.f_lon
-- 				,tb1.f_lat
-- 				,tb1.f_tm
-- 				,tb.t_tm_stamp
-- 				,tb1.f_tm_stamp
-- 				,tb.t_position
-- 				,tb1.f_position							
-- 		from cleaned_20190301 as tb 
-- 		join cleaned_20190301 as tb1  
-- 		on tb.card_id= tb1.card_id 
-- 	   		and tb.row_num=tb1.row_num-1  
-- 	   		and age(tb1.f_tm_stamp,tb.t_tm_stamp) < interval '30 minutes'
-- 			and ST_Distance(tb.t_position,tb1.f_position)<=300
-- 			and	
-- 			and abs(degrees(ST_Azimuth(tb.f_position,tb.t_position))-degrees(ST_Azimuth(tb1.f_position,tb1.t_position)))<170
-- 	);
 

/*----------------------换乘合并结束------------------------*/

------------6.25------------
--换乘记录
DROP TABLE IF EXISTS transfer_20190301;
CREATE TABLE transfer_20190301 AS
(
	SELECT	tb.f_line AS f_line_come
		,tb.fst_name AS fst_name_come
		,tb.t_line AS t_line_come
		,tb.tst_name AS tst_name_come
		,tb1.f_line AS f_line_back
		,tb1.fst_name AS fst_name_back
		,tb1.t_line AS t_line_back
		,tb1.tst_name AS tst_name_back
		,tb.f_tm_stamp AS f_tm_stamp_come
		,tb.t_tm_stamp AS t_tm_stamp_come
		,tb1.f_tm_stamp AS f_tm_stamp_back
		,tb1.t_tm_stamp AS t_tm_stamp_back
		,tb.f_position AS f_position_come
		,tb.t_position AS t_position_come
		,tb1.f_position AS f_position_back
		,tb1.t_position AS t_position_back
	from cleaned_20190301 as tb
	join cleaned_20190301 as tb1  
	on tb.card_id= tb1.card_id 
		AND tb.row_num=tb1.row_num-1
	WHERE age(tb1.f_tm_stamp,tb.t_tm_stamp) < interval '30 minutes'
		AND ST_Distance(tb.t_position,tb1.f_position)<=300
		AND	age(tb1.f_tm_stamp,tb.t_tm_stamp)<age(tb.t_tm_stamp,tb.f_tm_stamp)+age(tb1.t_tm_stamp,tb1.f_tm_stamp)
		AND	(	sqrt(ST_Distance(tb.f_position,tb1.t_position)^2+ST_Distance(tb.t_position,tb1.f_position)^2)>=2000
				OR 	(	sqrt(ST_Distance(tb.f_position,tb1.t_position)^2+ST_Distance(tb.t_position,tb1.f_position)^2)<2000
					AND	(	abs(degrees(ST_Azimuth(tb.f_position,tb.t_position))-degrees(ST_Azimuth(tb1.f_position,tb1.t_position)))<150
							OR	abs(degrees(ST_Azimuth(tb.f_position,tb.t_position))-degrees(ST_Azimuth(tb1.f_position,tb1.t_position)))>210
						)
					)
			)
)
;
--检查出行次数10次及以上的记录
SELECT *
FROM link_20190301_ as tb
GROUP BY card_id
HAVING count(*)>9
;
--检查刷卡次数15次及以上的记录
SELECT	tb.*
FROM	cleaned_20190301 AS tb
		LEFT JOIN
		(
		SELECT	card_id
		FROM	cleaned_20190301 
		GROUP BY card_id
		HAVING count(*)>=15
		) tb2
		ON	tb.card_id=tb2.card_id
WHERE	tb2.card_id IS NOT NULL
;

SELECT	card_id
		,sum(extract(epoch FROM age(tb.f_tm_stamp,tb.t_tm_stamp)) )AS duration
FROM cleaned_20190301 as tb
GROUP BY card_id
HAVING count(*)>15
--路上时长
SELECT extract(epoch FROM age(t_tm_stamp,f_tm_stamp))/3600 AS duration 
FROM link_20190301_;
--在公交上的时长
SELECT extract(epoch FROM age(t_tm_stamp,f_tm_stamp))/3600 AS duration 
FROM cleaned_20190301;

--公交出行量较大区域

------------6.20-------------
--检查link时间差小于0或出行距离>80KM的记录
select	card_id
		,ST_Distance(l.f_position,l.t_position) as trip_dist
		,age(l.t_tm_stamp,l.f_tm_stamp) as interval_
from	link_20190301_ as l
where	ST_Distance(l.f_position,l.t_position)>80000
		or	age(l.t_tm_stamp,l.f_tm_stamp)<0


------------6.18-------------
copy(
		select	c.card_id
				,stage_freq
				,trip_freq
		from	(
					(
						select	card_id
								,count(*) as stage_freq
						from	cleaned_20190301
						group by	card_id
					)c
					join	(
								select	card_id
										,count(*) as trip_freq
								from	link_20190301_
								group by	card_id
							)l
					on	c.card_id=l.card_id
				)
	)
to 'D:/Data/Sample/trip_freq.csv' (format csv, delimiter ',');



copy(
		select	card_id
				,trip_dist
				,extract(hour from interval_)+extract(minute from interval_)/60 as trip_dura
				
		from	(
					select	card_id
							,ST_Distance(l.f_position,l.t_position) as trip_dist
							,age(l.t_tm_stamp,l.f_tm_stamp) as interval_
					from	link_20190301_ as l
				)l
	)
to 'D:/Data/Sample/trip_dura_dist.csv' (format csv, delimiter ',');


copy(
		select (extract(hour from interval_)+extract(minute from interval_)/60) as interval_daily
		from(
				select age(cleaned_201903011.f_tm_stamp,cleaned_20190301.t_tm_stamp) as interval_ 
				from cleaned_20190301 
				inner join (select * from cleaned_20190301) cleaned_201903011  
				on cleaned_20190301.card_id= cleaned_201903011.card_id 
					and cleaned_20190301.row_num=cleaned_201903011.row_num-1
			)t1
)
to 'D:/Data/Sample/interval_between_stage.csv' (format csv, delimiter ',');

copy(
		select (extract(hour from interval_)+extract(minute from interval_)/60) as interval_daily
		from(
				select age(link_20190301_1.f_tm_stamp,link_20190301_.t_tm_stamp) as interval_ 
				from link_20190301_ 
				inner join (select * from link_20190301_) link_20190301_1  
				on link_20190301_.card_id= link_20190301_1.card_id 
					and link_20190301_.row_num=link_20190301_1.row_num-1
			)t1
)
to 'D:/Data/Sample/interval_between_trip.csv' (format csv, delimiter ',');

select	count(*) 
from	transfer_20190301_
where	substring(t_mode,'..$')!=substring(f_mode,'..$')

--transfer
copy(
		select (extract(hour from interval_)+extract(minute from interval_)/60) as interval_daily
		from(
				select age(link_20190301_1.f_tm_stamp,link_20190301_.t_tm_stamp) as interval_ 
				from link_20190301_ 
				inner join (select * from link_20190301_) link_20190301_1  
				on link_20190301_.card_id= link_20190301_1.card_id 
					and link_20190301_.row_num=link_20190301_1.row_num-1
			)t1
)
to 'D:/Data/Sample/interval_between_trip.csv' (format csv, delimiter ',');
		
select	count(*) 
from	link_20190301_
where	t_line!=f_line

select	count(*) 
from	link_20190301_
where	substring(t_mode,'..$')!=substring(f_mode,'..$')

select	count(*) 
from	link_20190301_
where	substring(t_mode,'..$')=substring(f_mode,'..$')
		and	t_line!=f_line

create table split_20190302(
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
------------6.10-------------
--absolute accessibility:drive_dist/transit_dura
select avg(drive_dist/transit_dura) 
from duration_distance 
group by (o_lon,o_lat)
--relative accessibility:(transit_dura-drive_dura)/drive_dist
select avg(transit_dura/drive_dist) 
from duration_distance 
group by (o_lon,o_lat)

------------5.31-------------
--generate clusters
drop table if exists clusters;
 create table clusters
 as		(
 			select geom
 					,st_ClusterKMeans(geom,3) over (partition by tazid) as cluster_id
					,tazid
 			from	(
						select	(st_dump(st_GeneratePoints(geom,100))).geom as geom
								,tazid
						from 	taz_in_core_area
					) random_pts
 		)
 ;
 
drop table if exists center_point;
 create table center_point
 as		(
 			select st_centroid(st_collect(geom))  as geom
					,tazid
 					,cluster_id
 			from	clusters
 			group 	by	(tazid,cluster_id)	
 		)
 ;
 
drop table if exists voronoi;
 create table voronoi
 as		(
 			select (ST_Dump(ST_VoronoiPolygons(ST_collect(geom),200))).geom  as geom
					,tazid
 			from	center_point
 			group 	by	(tazid)	
 		)
 ;
 
drop table if exists voronoi_its;
 create table voronoi_its
 as		(
 			select ST_Intersection(taz.geom, vor.geom)  as geom
					,taz.tazid
 			from	taz_in_core_area as taz
			join	voronoi as vor
			on		taz.tazid=vor.tazid
 		)
 ;
 
drop table if exists pts_pair;
 create table pts_pair
 as		(
 			select t1.geom as p1
 					,t2.geom as p2
					,t1.tazid as p1_taz
					,t2.tazid as p2_taz
					,st_distance(t1.geom,t2.geom) as distance
 			from	center_point as t1
			join	center_point as t2
			on		t1.geom<t2.geom
			where	st_distance(t1.geom,t2.geom)>1000
 				
 		)
 ;
 
copy	(
			select st_astext(st_transform(p1,4326)::geography),st_astext(st_transform(p2,4326)::geography)
			from	pts_pair_57
		)
to 'D:/Data/pts_pair.txt'
;

drop table if exists station_20190301;
create table station_20190301
as		(
			select distinct mode,line,dir,st_num,name,position
			from	(
						select distinct substring(f_mode,'..$') as mode
									,f_line as line
									,f_dir as dir
									,to_number(fst_num,'999') as st_num
									,fst_name as name
									,f_position as position
						from cleaned_20190301
						union
						select	distinct substring(t_mode,'..$') as mode
									,t_line as line
									,t_dir as dir
									,to_number(tst_num,'999') as st_num
									,tst_name as name
									,t_position as position
						from cleaned_20190301
					)a
			order by mode,line,dir,st_num,name,position
		)
;
------------5.30统计公交站情况-----------------
--需要公交站字段：线路编号，公交站编号，公交站名字，经纬度

------------5.23生成网格---------------------
create table core_area_poly
as			(
				select st_polygonfromtext('POLYGON((116.782512 39.942293,116.781832 39.937682,116.779227 39.935356,116.742085 39.923493,116.740699 39.918116,116.74266 39.90859,116.738404 39.888296,116.739754 39.885002,116.745435 39.880081,116.749063 39.865424,116.755008 39.860195,116.777674 39.85923,116.781506 39.856864,116.782346 39.851378,116.778524 39.846371,116.778046 39.841385,116.775261 39.837794,116.769665 39.837586,116.743784 39.851823,116.735249 39.854141,116.730239 39.853056,116.727835 39.8475,116.72405 39.844792,116.719469 39.845603,116.715224 39.849371,116.711525 39.849274,116.706354 39.842228,116.698411 39.840667,116.69191 39.833,116.681961 39.831251,116.664079 39.816197,116.66055 39.809953,116.65384 39.808412,116.641152 39.79878,116.637632 39.789439,116.630143 39.786523,116.628066 39.784148,116.617761 39.764504,116.615485 39.75531,116.612432 39.753653,116.595782 39.752078,116.592708 39.750294,116.591573 39.746836,116.594531 39.738076,116.591141 39.733579,116.579918 39.734451,116.577037 39.732648,116.574389 39.728418,116.56988 39.726937,116.566554 39.72832,116.561646 39.733392,116.546861 39.731789,116.540772 39.734151,116.536791 39.738295,116.533729 39.738861,116.523328 39.735239,116.504118 39.732639,116.487784 39.725818,116.47236 39.722425,116.466543 39.718562,116.458915 39.718715,116.454628 39.711738,116.450526 39.709655,116.446154 39.711089,116.442629 39.716119,116.43936 39.717584,116.417552 39.716563,116.412334 39.711001,116.409117 39.709642,116.405689 39.710304,116.398951 39.715839,116.38832 39.715909,116.383182 39.711261,116.366359 39.703065,116.336649 39.695552,116.330046 39.678976,116.336198 39.67054,116.336075 39.666072,116.3273 39.658965,116.323972 39.658012,116.319774 39.659806,116.315672 39.665787,116.321011 39.679294,116.316623 39.686295,116.308569 39.691001,116.306982 39.69536,116.308174 39.698672,116.314702 39.704024,116.315978 39.714396,116.323177 39.718369,116.324239 39.723073,116.322766 39.727028,116.317118 39.733068,116.315384 39.740061,116.308923 39.743627,116.303187 39.751151,116.299637 39.768716,116.295679 39.770919,116.284437 39.768308,116.281551 39.766113,116.279011 39.761135,116.275798 39.759674,116.27232 39.760272,116.267601 39.764195,116.259456 39.766428,116.257906 39.769515,116.258315 39.772946,116.264454 39.779337,116.265673 39.782767,116.261246 39.796229,116.259572 39.807289,116.245025 39.824989,116.233794 39.829501,116.22402 39.83864,116.219887 39.839199,116.210466 39.837051,116.2026 39.828192,116.192996 39.826944,116.188607 39.828562,116.184621 39.839192,116.185874 39.842316,116.190279 39.846558,116.19071 39.852784,116.194351 39.858255,116.193706 39.862026,116.188033 39.866559,116.187058 39.873267,116.185043 39.876146,116.179914 39.878046,116.1688 39.876633,116.160429 39.883413,116.159154 39.886691,116.159936 39.89012,116.162505 39.892522,116.169635 39.89577,116.180259 39.897602,116.181892 39.903071,116.176266 39.908927,116.174227 39.916006,116.171791 39.918531,116.154254 39.9193,116.151204 39.920952,116.149577 39.924016,116.149164 39.932928,116.14993 39.936351,116.153587 39.939179,116.164265 39.937832,116.17092 39.940689,116.177649 39.939433,116.192269 39.940908,116.195049 39.942541,116.198675 39.948048,116.206782 39.949359,116.209703 39.95139,116.215869 39.983557,116.213738 39.986789,116.204187 39.988662,116.200616 39.991647,116.200253 39.996287,116.206111 40.004257,116.211794 40.005103,116.21669 40.000138,116.220527 39.999299,116.248018 40.007295,116.258166 40.008195,116.261256 40.010841,116.261874 40.015774,116.258199 40.024811,116.254831 40.026405,116.248695 40.023955,116.2441 40.024823,116.239814 40.029437,116.238956 40.036865,116.231365 40.041142,116.229604 40.045537,116.233215 40.051557,116.242064 40.056475,116.244083 40.059531,116.243287 40.063496,116.236426 40.069348,116.235903 40.072701,116.237282 40.075802,116.257792 40.083014,116.26073 40.085037,116.26201 40.088705,116.259813 40.092548,116.239694 40.107527,116.236526 40.113731,116.230319 40.11721,116.228961 40.122863,116.237421 40.131064,116.241 40.13086,116.247304 40.126333,116.253654 40.127352,116.255481 40.13044,116.255059 40.13772,116.259491 40.142834,116.263129 40.143431,116.268957 40.141607,116.27552 40.133555,116.276777 40.128044,116.279896 40.124194,116.28108 40.120078,116.301969 40.102823,116.305008 40.103209,116.309697 40.107923,116.313016 40.108714,116.322472 40.103251,116.346474 40.104314,116.349647 40.108438,116.35129 40.116954,116.357967 40.119852,116.371901 40.118308,116.37638 40.113744,116.381339 40.111236,116.384206 40.111759,116.388386 40.115187,116.391752 40.115577,116.40083 40.108431,116.4046 40.109174,116.409627 40.113676,116.414233 40.113212,116.417138 40.109608,116.418674 40.098551,116.42217 40.094783,116.429776 40.091371,116.439656 40.092918,116.448704 40.087807,116.450823 40.084985,116.450814 40.079535,116.446195 40.073543,116.447396 40.066481,116.450714 40.059805,116.447188 40.052802,116.447868 40.039416,116.452935 40.037376,116.460746 40.041079,116.46742 40.038703,116.474133 40.042043,116.477252 40.047142,116.477146 40.052525,116.474258 40.058956,116.476992 40.06552,116.476436 40.08287,116.477424 40.086387,116.485167 40.09015,116.487872 40.0968,116.490507 40.099275,116.512963 40.100772,116.517178 40.102745,116.517859 40.11075,116.511783 40.118741,116.51381 40.124185,116.52429 40.126712,116.535162 40.12727,116.537692 40.129034,116.54001 40.134269,116.544005 40.136366,116.574669 40.135736,116.577472 40.138673,116.577578 40.144246,116.580285 40.148053,116.595947 40.148795,116.62889 40.166222,116.637139 40.179759,116.641088 40.181801,116.645358 40.180564,116.649928 40.175863,116.6736 40.175641,116.6775 40.172978,116.677857 40.167176,116.673995 40.162629,116.673537 40.156333,116.668227 40.145931,116.665036 40.142448,116.664619 40.138744,116.673119 40.127449,116.679291 40.122758,116.688467 40.120251,116.690813 40.114934,116.688155 40.109298,116.681453 40.102967,116.670578 40.103071,116.66646 40.099826,116.666671 40.071225,116.671384 40.064677,116.671685 40.061278,116.667281 40.055607,116.660673 40.053037,116.65768 40.049427,116.649526 40.048456,116.646672 40.046029,116.646136 40.043238,116.64979 40.028427,116.648027 40.023216,116.640752 40.021111,116.636895 40.017667,116.632431 40.016588,116.622583 40.017069,116.611927 40.020282,116.609126 40.018921,116.606615 40.015095,116.606053 40.010161,116.612821 40.003494,116.613745 39.993096,116.617611 39.986808,116.61803 39.98323,116.616324 39.980057,116.611652 39.97705,116.592147 39.973425,116.588607 39.971126,116.587842 39.968013,116.589645 39.952886,116.594584 39.940138,116.598638 39.937318,116.612804 39.937381,116.619726 39.932812,116.627015 39.935276,116.633922 39.933837,116.640037 39.937121,116.646583 39.936843,116.650166 39.93866,116.651652 39.941981,116.651638 39.950715,116.655198 39.953988,116.662471 39.953631,116.669475 39.958422,116.696286 39.959106,116.701359 39.964108,116.704532 39.96519,116.708715 39.963747,116.713243 39.957694,116.721025 39.956145,116.726238 39.949139,116.747578 39.949864,116.772161 39.94787,116.779805 39.94594,116.782512 39.942293))',4326)
				as poly
			)
;

select st_distance(a1.uprg,a1.lwlf)
from 	(
			select st_setsrid(st_point(st_xmax(st_extent(poly)),st_ymax(st_extent(poly))),4326)::geography as uprg
					,st_setsrid(st_point(st_xmin(st_extent(poly)),st_ymin(st_extent(poly))),4326)::geography as lwlf
			from	core_area_poly
		)a1
;

drop table if exists grid_cell;
create table grid_cell
as		(
			select	st_translate(a1.first_square, x_series, y_series) as grid, st_translate(a1.first_point, x_series, y_series) as center_point
			from	generate_series(0, 200*270, 200) as x_series, 
					generate_series(0, 200*290, 200) as y_series, 
					( 
						select st_geomFromText('POLYGON((427301 4390019,427301 4390219,427501 4390219,427501 4390019,427301 4390019))',32650)::geometry as first_square
								,st_geomFromText('POINT(427401 4390119)',32650)::geometry as first_point
					) as a1
		)	
; 

drop table if exists grid_its;
create table grid_its
as		(
			select	grid_cell.gid as gid
					,st_intersection(grid_cell.grid,core_area_utm.poly) as poly
			from	grid_cell,core_area_utm
		)	
; 

drop table if exists grid_its_4326;
 create table grid_its_4326
 as		(
 			select gid
 					,st_transform(center_point,4326)::geography as center_point
 					,st_transform(poly,4326)::geography as poly
 			from	grid_its
 		)
 ;
 
copy	(
			select st_astext(center_point) from grid_its_4326 as center_point
		)
to 'D:/Data/center_point.txt' 
