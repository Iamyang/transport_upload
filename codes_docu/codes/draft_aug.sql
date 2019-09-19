--8.28-------------

--8.27-------------
DROP TABLE IF EXISTS cleaned_sample;
CREATE TABLE cleaned_sample AS
(
    SELECT  *
    FROM    ic_trsform
    WHERE NOT  ST_Distance(f_position,t_position)<1
    AND NOT age(t_tm,f_tm)<='0'        
)

--8.11-------------
DROP TABLE IF EXISTS ic_sample;
CREATE TABLE ic_sample
(
    card_id CHARACTER VARYING, 
    f_mode CHARACTER VARYING,
    f_line CHARACTER VARYING,
    f_dir CHARACTER VARYING,
    fst_num INTEGER,
    fst_name CHARACTER VARYING,
    f_lon DOUBLE PRECISION,
    f_lat DOUBLE PRECISION,
    f_tm CHARACTER VARYING,
    t_mode CHARACTER VARYING,
    t_line CHARACTER VARYING,
    t_dir CHARACTER VARYING,
    tst_num INTEGER,
    tst_name CHARACTER VARYING,
    t_lon DOUBLE PRECISION,
    t_lat DOUBLE PRECISION,
    t_tm CHARACTER VARYING,
    ds CHARACTER VARYING
);
COPY ic_sample
FROM 'E:/RESEARCH/Transportation/Data/bjjw_ic_sample_data.txt'(format csv, delimiter ',', header TRUE)

DROP TABLE IF EXISTS ic_trsform;
CREATE TABLE ic_trsform AS
(
    SELECT  card_id
            ,f_mode 
            ,f_line
            ,f_dir
            ,fst_num
            ,fst_name
            ,to_timestamp(f_tm,'YYYYMMDDHH24MISS') AS f_tm
            ,t_mode
            ,t_line
            ,t_dir
            ,tst_num
            ,tst_name
            ,to_timestamp(t_tm,'YYYYMMDDHH24MISS') AS t_tm
            ,to_timestamp(ds,'YYYYMMDD') AS ds
            ,ST_Transform(ST_GeomFromText('POINT('||f_lon||' '||f_lat||')',4326),32650)::geometry(Point,32650) AS f_position
            ,ST_Transform(ST_GeomFromText('POINT('||t_lon||' '||t_lat||')',4326),32650)::geometry(Point,32650) AS t_position
    FROM    ic_sample
)



--8.8--------------
--检查速度大于15m/s的情况
--提前刷卡的情况，包括上下车点一样的记录。
--重新合并换乘，然后统计各个格网内的公交站数目
DROP TABLE IF EXISTS speedlgt15;
CREATE TABLE speedlgt15 AS	
(
    SELECT	*
    FROM	(
            SELECT  t1.card_id
                    ,t1.f_mode AS mode1
                    ,t1.f_line AS line1
                    ,t1.f_tm_stamp AS f_tm_stamp1
                    ,t1.t_tm_stamp AS t_tm_stamp1
                    ,t1.fst_name AS fst_name1
                    ,t1.tst_name AS tst_name1
                    ,t2.f_mode AS mode2
                    ,t2.f_line AS line2
                    ,t2.f_tm_stamp AS f_tm_stamp2
                    ,t2.t_tm_stamp AS t_tm_stamp2
                    ,t2.fst_name AS fst_name2
                    ,t2.tst_name AS tst_name2
                    ,t1.row_num AS r1
                    ,t2.row_num AS r2
                    ,extract(epoch FROM age(t2.f_tm_stamp,t1.t_tm_stamp)) AS t_gap	
                    ,ST_distance(t1.t_position,t2.f_position) AS dist
                    ,ST_distance(t1.t_position,t2.f_position)/extract(epoch FROM age(t2.f_tm_stamp,t1.t_tm_stamp)) AS speed
            FROM cleaned_temp AS t1
            INNER JOIN cleaned_temp AS t2  
            ON t1.card_id= t2.card_id 
                AND t1.row_num=t2.row_num-1
            )tb 
    WHERE	(t_gap>0 AND t_gap<30*60 AND speed>15) --异常记录（6）：30分钟内速度>15m/s
    --OR t_gap<=0 --异常记录（5）：时间差小于1秒
)
;

--8.1--------------
CREATE OR REPLACE FUNCTION equal_point(
   dura_ar interval[],
   index_ar integer[]
) 
RETURNS integer AS $$  
DECLARE  
    equal_idx integer:=1;
    previous INTERVAL :='24 hours' ;
    currt INTERVAL :='24 hours';
	arr_length integer:=array_length(dura_ar,1);
BEGIN
    FOR i IN 1..arr_length LOOP
        currt :=(SELECT SUM(s) FROM UNNEST(dura_ar[1:i]) s)-(SELECT SUM(s) FROM UNNEST(dura_ar[i+1:arr_length]) s);
        IF (currt< interval '0') THEN
			currt :=-currt;
		END IF;
		IF (currt<previous) THEN
                equal_idx:=i;
				previous:=currt;
        ELSE
                EXIT;
        END IF;
   END LOOP;
   RETURN index_ar[equal_idx];  
END;  
$$ LANGUAGE plpgsql;

with inpt as(
	select	array_agg(age(t_tm_stamp,f_tm_stamp) ) as dura_ar
			,array_agg(row_num) as index_ar
	from return_trips
	group by card_id
)
select equal_point(dura_ar,index_ar)
from inpt