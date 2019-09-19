/*
	本代码实现：单次换乘合并,对于连续换乘情况视作两条换乘记录。仅存储换乘记录，不包括未发生换乘行为的记录
	数据输入：清洗后的乘车记录cleaned_20190301
	数据输出：合并后换乘的记录transfer_20190301
	
	判断换乘的基本规则：
	1.  中转时间＜30分钟，中转距离＜300m。
	2. “中转时间/公交乘车时间”<1
	3. 对于前后乘车流距离在2KM内的，选取角度<150°；距离在2KM外的，无要求。
*/
DROP TABLE IF EXISTS transfer_20190301;
CREATE TABLE transfer_20190301 AS
(
	SELECT	tb.card_id
		,tb.f_mode AS mode_come
		,tb.f_line AS f_line_come
		,tb.fst_name AS fst_name_come
		,tb.t_line AS t_line_come
		,tb.tst_name AS tst_name_come
		,tb1.f_mode AS mode_back
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
	WHERE age(tb1.f_tm_stamp,tb.t_tm_stamp) < interval '30 minutes' --中转时间＜30分钟
		AND ST_Distance(tb.t_position,tb1.f_position)<=300	--中转距离＜300m
		AND	age(tb1.f_tm_stamp,tb.t_tm_stamp)<(age(tb.t_tm_stamp,tb.f_tm_stamp)+age(tb1.t_tm_stamp,tb1.f_tm_stamp)) --“中转时间/公交乘车时间”<1
		--流距离在2KM内的，选取角度<150°；距离在2KM外的，无要求。
		AND	(	sqrt(ST_Distance(tb.f_position,tb1.t_position)^2+ST_Distance(tb.t_position,tb1.f_position)^2)>=2000
				OR 	(	sqrt(ST_Distance(tb.f_position,tb1.t_position)^2+ST_Distance(tb.t_position,tb1.f_position)^2)<2000
					AND	(	abs(degrees(ST_Azimuth(tb.f_position,tb.t_position))-degrees(ST_Azimuth(tb1.f_position,tb1.t_position)))<150
							--由于计算的是方位角之差，对于大于180度的角度，其实际角度是360°-degree。即>210°的方位角之差的实际角度是小于150°
							OR	abs(degrees(ST_Azimuth(tb.f_position,tb.t_position))-degrees(ST_Azimuth(tb1.f_position,tb1.t_position)))>210 
						)
					)
			)
)
;

--公交地铁接驳热点
SELECT	tst_name_come
		,tst_name_back
		,count(*) AS cnt
FROM transfer_20190301
WHERE	mode_come!=mode_back
GROUP BY tst_name_come,tst_name_back
ORDER BY count(*) desc
;

