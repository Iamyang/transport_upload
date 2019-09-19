/*
	本代码实现：换乘合并
	数据输入：清洗后的乘车记录cleaned_20190301
	数据输出：合并后换乘的记录link_20190301. 后续计算基于link_20190301
	经统计，约有11%的记录被合并
	
	判断换乘的基本规则：
		1.  中转时间＜30分钟，中转距离＜300m。
		2. “中转时间/公交乘车时间”<1
		3. 前后两次出行，前一次上车与下一次下车距离大于300m
		按照前3天合并换乘后，若出现起终点一样（距离不大于300m)即往返的情况，对原记录进行拆分，
		原则是：往程与返程时间差最小


	具体思路：
		以一个人的乘车记录为例：
			上车点 下车点 记录编号row_num（编号按照上车时间由早到晚排列）
			a 		b		1
			b 		c		2
			c 		d		3
			e 		f		4
			f		h		5
			h		e		6
			i		j		7
		（1）首先找到两两换乘的记录：按照时间将每条记录与下一条记录做连接，若满足换乘规则，则存储其行号到link_count中
			对于例子得到记录：
			1 2
			2 3
			4 5
			5 6
		（2）换乘可能存在多次，需要找到换乘的起点和终点，基于link_count，结果存储到link_count2中
			对于例子得到记录：
			start_row	end_row
			1 		  	3
			4			6
		（3）取乘车记录中行号为起点编号的起点信息，行号为终点编号的终点信息，得到换乘合并后的记录，存储到link_temp中
			对于例子得到记录：
			a	d
			e	e
		（4）合并后可能存在起终点相同即往返程的情况，需要检查。首先把起终点不同情况筛选出来，存储到link_true1中，
			原则是起终点距离大于300m，这部分是正常的出行链。对于例子：
			a	d
		（5）对于往返程的情况，首先从cleaned_20190301中找到原始记录，存储到return_trips中，
			对于例子：
			e	f	4
			f	h	5
			h	e	6
		（6）计算往返程的分割点，原则是往程和返程的乘车时间差最小
			对于例子：
			e	f	4	40分钟
			f	h	5	25分钟
			h	e	6	17分钟
			则应从4和5中间断开，记录到return_counts中：
			4	4	6
		（7）得到往返程出行链，记录到return_link中，
			对于例子：
			e	h
			h	e
		（8）除去换乘记录，有些记录未发生换乘，筛选出来存储到 link_temp2中
			对于例子得到1条记录：
			i	j
		（9）合并换乘出行link_temp和非换乘出行link_temp2，得到完整的出行链，存储到link_20190301
			对于例子得到2条记录：
			a	d
			e	h
			h	e
			i	j
		
		（10）删除临时表link_count，link_count2，link_temp，link_true1, 
			return_trips, return_counts, return_link, link_temp2
*/

--创建临时表，加入行编号
DROP TABLE IF EXISTS cleaned_temp;
CREATE TABLE cleaned_temp 
AS  (
        SELECT  *
                ,ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY f_tm) AS row_num
        FROM    cleaned_sample
);
/*创建临时表，仅存储前后两次换乘出行记录的行号。判断换乘规则：时间间隔小于30分钟，距离间隔小于300m,方向角之差小于170度*/ 
drop table if exists link_count;
create table link_count 
as	(
		SELECT tb.card_id
			,tb.row_num as r1
			,tb1.row_num as r2
			,row_number()over(partition by tb.card_id order by tb.row_num) as row_num
		FROM cleaned_temp as tb
		join cleaned_temp as tb1  
		on tb.card_id= tb1.card_id 
	   		AND tb.row_num=tb1.row_num-1
            AND age(tb1.f_tm,tb.t_tm) < interval '30 minutes' --中转时间＜30分钟
            AND ST_Distance(tb.t_position,tb1.f_position)<=300	--中转距离＜300m
            AND	age(tb1.f_tm,tb.t_tm)<(age(tb.t_tm,tb.f_tm)+age(tb1.t_tm,tb1.f_tm)) --“中转时间/公交乘车时间”<1
            AND	ST_Distance(tb.f_position,tb1.t_position)>300 --前后两次出行，前一次上车与下一次下车距离大于300m
            --流距离在2KM内的，选取角度<150°；距离在2KM外的，无要求。
            -- AND	(	sqrt(ST_Distance(tb.f_position,tb1.t_position)^2+ST_Distance(tb.t_position,tb1.f_position)^2)>=2000
            -- 		OR 	(	sqrt(ST_Distance(tb.f_position,tb1.t_position)^2+ST_Distance(tb.t_position,tb1.f_position)^2)<2000
            -- 			AND	(	abs(degrees(ST_Azimuth(tb.f_position,tb.t_position))-degrees(ST_Azimuth(tb1.f_position,tb1.t_position)))<150
            -- 					--由于计算的是方位角之差，对于大于180度的角度，其实际角度是360°-degree。即>210°的方位角之差的实际角度是小于150°
            -- 					OR	abs(degrees(ST_Azimuth(tb.f_position,tb.t_position))-degrees(ST_Azimuth(tb1.f_position,tb1.t_position)))>210 
            -- 				)
            -- 			)
            -- 	)  
	);
 
/*创建临时表，存储换乘的起始行号*/ 
drop table if exists link_count2;
create table link_count2 
as	(
		select card_id
			,min(r1) as start_row
			,max(r2) as end_row 
		from link_count
		group by card_id, r1-row_num
	);

/*创建临时表，存储换乘出行的合并后的出行记录*/ 
drop table if exists link_temp;
create table link_temp 
as	(
		select tb.card_id
				,tb.f_mode
				,tb.f_line
				,tb.f_dir
				,tb.fst_num
				,tb.fst_name
				,tb1.t_mode
				,tb1.t_line
				,tb1.t_dir
				,tb1.tst_num
				,tb1.tst_name
				,tb.f_tm
				,tb1.t_tm
				,tb.f_position
				,tb1.t_position
				,link_count2.start_row
				,link_count2.end_row
		from cleaned_temp as tb 
		inner join link_count2 
		on tb.card_id=link_count2.card_id 
			and tb.row_num=link_count2.start_row
		inner join cleaned_temp as tb1 
		on tb1.card_id=link_count2.card_id 
			and tb1.row_num=link_count2.end_row
);

DROP TABLE IF EXISTS link_true1;
CREATE TABLE link_true1 AS
(
	SELECT	*
	FROM	link_temp
	WHERE	ST_Distance(f_position,t_position)>300
);

DROP TABLE IF EXISTS return_trips;
CREATE TABLE return_trips AS
(
	WITH ret AS(
		SELECT	*
		FROM	link_temp
		WHERE	ST_Distance(f_position,t_position)<=300
	)
	SELECT	cl.*
	FROM	cleaned_temp AS cl
	JOIN	ret 
	ON	cl.card_id=ret.card_id
	AND	cl.row_num BETWEEN ret.start_row AND ret.end_row
);

DROP TABLE IF EXISTS return_counts;
CREATE TABLE return_counts AS
(
	with inpt as(
        select	array_agg(age(t_tm,f_tm) ) as dura_ar
                ,cast (array_agg(row_num) as integer[]) as index_ar
                ,min(row_num) as min_row
                ,max(row_num) as max_row
                ,card_id
        from return_trips
        group by card_id
	)
	select	min_row
			,equal_point(dura_ar,index_ar) as equal_row
			,max_row
			,card_id	
	from inpt
);

drop table if exists return_link;
create table return_link 
as	(
		select tb.card_id
				,tb.f_mode
				,tb.f_line
				,tb.f_dir
				,tb.fst_num
				,tb.fst_name
				,tb1.t_mode
				,tb1.t_line
				,tb1.t_dir
				,tb1.tst_num
				,tb1.tst_name
				,tb.f_tm
				,tb1.t_tm
				,tb.f_position
				,tb1.t_position
				,co.min_row as start_row
				,co.equal_row as end_row
		from return_trips as tb 
		inner join return_counts as co
		on tb.card_id=co.card_id 
			and tb.row_num=co.min_row
		inner join return_trips as tb1 
		on tb1.card_id=co.card_id 
			and tb1.row_num=co.equal_row
		UNION ALL
		select tb.card_id
				,tb.f_mode
				,tb.f_line
				,tb.f_dir
				,tb.fst_num
				,tb.fst_name
				,tb1.t_mode
				,tb1.t_line
				,tb1.t_dir
				,tb1.tst_num
				,tb1.tst_name
				,tb.f_tm
				,tb1.t_tm
				,tb.f_position
				,tb1.t_position
				,co.equal_row+1 as start_row
				,co.max_row as end_row
		from return_trips as tb1 
		inner join return_counts as co
		on tb1.card_id=co.card_id 
			and tb1.row_num=co.max_row
		inner join return_trips as tb 
		on tb.card_id=co.card_id 
			and tb.row_num=co.equal_row+1
		ORDER BY card_id,f_tm
);

/*创建临时表，存储非换乘出行的出行记录*/ 
drop table if exists link_temp2;
create table link_temp2 
as	(
        WITH  excpt AS(
            SELECT  card_id
                    ,array_agg(r1) AS r1_agg
                    ,array_agg(r2) AS r2_agg
            FROM    link_count
            GROUP BY card_id
        )
		select	tb.card_id
				,tb.f_mode
				,tb.f_line
				,tb.f_dir
				,tb.fst_num
				,tb.fst_name
				,tb.t_mode
				,tb.t_line
				,tb.t_dir
				,tb.tst_num
				,tb.tst_name
				,tb.f_tm
				,tb.t_tm
				,tb.f_position
				,tb.t_position
				,tb.row_num AS start_row
				,tb.row_num AS end_row
		from cleaned_temp as tb
        LEFT JOIN excpt AS tb1 
        ON  tb.card_id=tb1.card_id
            AND NOT tb.row_num = ANY(tb1.r1_agg)
            AND NOT tb.row_num = ANY(tb1.r2_agg)
		WHERE tb1.card_id IS NOT NULL
	);

--合并换乘出行和非换乘出行的记录
--加入row_num，存储最后的结果到输出表link_20190301_中
drop table if exists link_sample;
create table link_sample 
as	(
		SELECT	*
		FROM	link_true1
		UNION ALL	
		SELECT	*
		FROM	return_link
		UNION ALL	
		SELECT	*
		FROM	link_temp2
		
	);

-- --异常用户：出行次数10次及以上
-- drop table if exists abnormal;
-- create table abnormal
-- as	(
-- 		select card_id
-- 		from link_20190301	as t1
-- 		group by card_id 
-- 		having	count(*)>9
-- 	);
-- --删除异常用户，出行链表	
-- delete from link_20190301
-- using	abnormal
-- where	link_20190301.card_id=abnormal.card_id;

	
/*删除临时表*/
drop table if exists cleaned_temp;
drop table if exists link_count;
drop table if exists link_count2;
drop table if exists link_temp;
drop table if exists link_true1;
drop table if exists return_trips;
drop table if exists return_counts;
drop table if exists return_link;
drop table if exists link_temp2;




/*----------------------换乘合并结束------------------------*/

