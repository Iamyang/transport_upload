/*
	本代码实现：数据清洗和一个统计
	数据输入：初始刷卡数据split_20190301
	数据输出：清洗后的数据cleaned_20190301
	数据清洗：
		共有6种异常数据：
			（1）上下车点一样的记录。
                注: 1）存在站点标号实为同一个但表示不同的情况，如“05”和“5”
                    2）存在上下车点名字一样但不同线路的情况，如从地铁2号线的东直门站进入，从地铁13号线的东直门站出
                    因此，无法通过线路站点编号相同剔除所有异常，采取上下车点距离小于1m的条件做过滤
			（2）下车时间超过24点
			（3）上下车时间小于1秒
			（4）上下车时间小于60秒，且乘车站数为2站及以上
			（5）下一次上车时间与上一次下车时间间隔过短以至于不可能发生换车，此处取为1秒
			（6）下一次上车与上一次下车时间间隔较短但两个站点距离间隔很大以至于不可能发生换车，
				此处取换车时间间隔30分钟内，速度大于15m/s。
		为了辅助清洗，添加了5个新字段，这5个字段在之后的处理中仍然有用：
			上下车时间为日期类型的字段
			上下车点为postgis geography类型的字段
			为每个人乘车次数计数的字段row_num
	清洗结束后，约有11%的记录被删去
*/

/*------------------------------数据清洗----------------------------*/

/*删除（1）-（4）异常记录，剩下记录暂时保存到cleaned_temp中*/
--18 min
drop table if exists cleaned_temp;
create table cleaned_temp as 
(
    SELECT  *
            ,ROW_NUMBER() over(partition by card_id order by f_tm) as row_num /*添加每个人乘车次数计数的新列row_num，例如：一个人共有5次乘车，则五条记录的row_num为1 2 3 4 5，排序规则是上车时间由早到晚*/
    FROM    split_trsform 
    WHERE NOT ST_Distance(f_position,t_position)<1 --（1）删除上下车点一样的记录
    AND NOT extract(hour FROM t_tm)>23 --（2）下车时间超过24点
    AND	NOT	age(t_tm,f_tm)<='0' --（3）删除上下车时间差<=0秒
    --AND	NOT	(age(to_timestamp(t_tm,'YYYYMMDDHH24MISS'),to_timestamp(f_tm,'YYYYMMDDHH24MISS'))<'60' and (cast(tst_num as integer) -cast(fst_num as integer))>1) --（4）上下车时间小于60秒且乘车站数2站及以上的记录

);

/*异常记录
（5）下一次上车时间早于或等于上一次下车时间
（6）下一次上车时间与上一次下车时间间隔小于30分钟且速度大于25m/s：
*/
DROP TABLE IF EXISTS abnormal;
CREATE TABLE abnormal AS	
(
    SELECT	card_id
            ,r1
            ,r2
    FROM	(
            SELECT t1.card_id
                ,t1.row_num AS r1
                ,t2.row_num AS r2
                ,extract(epoch FROM age(t2.f_tm,t1.t_tm)) AS t_gap	
                ,ST_distance(t1.t_position,t2.f_position) AS dist
            FROM cleaned_temp AS t1
            INNER JOIN cleaned_temp AS t2  
            ON t1.card_id= t2.card_id 
                AND t1.row_num=t2.row_num-1 
            )tb 
    WHERE	t_gap<=0 --异常记录（5）：时间差<=0秒
            OR  (t_gap>0 AND t_gap<30*60 AND dist/t_gap>25) --异常记录（6）：30分钟内速度>15m/s
);
--最终表格cleaned_20190301
drop table if exists cleaned_20190301;
create table cleaned_20190301 as
(
    select	*
            ,row_number() over(partition by card_id order by f_tm) as row_num
    from	(
            SELECT	t.card_id
                    ,t.f_mode
                    ,t.f_line
                    ,t.f_dir
                    ,t.fst_num
                    ,t.fst_name
                    ,t.t_mode
                    ,t.t_line
                    ,t.t_dir
                    ,t.tst_num
                    ,t.tst_name
                    ,t.f_tm
                    ,t.t_tm
                    ,t.f_position
                    ,t.t_position
            FROM	cleaned_temp AS t
            LEFT JOIN abnormal
            ON 	t.card_id=abnormal.card_id 
                and (t.row_num=abnormal.r1 or t.row_num=abnormal.r2)
            WHERE  abnormal.card_id IS NULL --去除异常记录（5）（6）
    )a
)
;

--删去临时表
drop table if exists cleaned_temp;
drop table if exists abnormal;
--统计清洗后记录数
select count(*)
from cleaned_20190301
;
/*------------------------------数据清洗结束----------------------------*/	




