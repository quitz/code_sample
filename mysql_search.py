#coding:gbk
import os
import sys
import time
import string
import MySQLdb
import random
import thread
import util
import common_method


db_file     = "../conf_file/common_info/database_info"
sum_file    = "../conf_file/common_info/summary_table_info"
tb_file     = "../conf_file/cuid_log_err_conf/table_can_read"
top_page    = "../conf_file/common_info/top_page_info_need"

qvt_size_dis    = "../conf_file/qt_version_conf/qvt_size_distribution"
qvt_time_dis    = "../conf_file/qt_version_conf/qvt_time_distribution"
qvtm_time_dis   = "../conf_file/qt_version_conf/qvtm_time_distribution"
city_dist_dis   = "../conf_file/city_info_conf/city_distance_distribution"
sy_prefer_mrs_type_file  = "../conf_file/sy_prefer_mrs_conf/sy_prefer_mrs_type"
resid_filter_qt_version  = "../conf_file/resid_info_conf/resid_filter_qt_version"
from_resid_port_file = "../conf_file/common_info/from_resid_to_port"
type_flag_type_file = "../conf_file/naviure_info_conf/type_flag_type"
prefer_size_dis    = "../conf_file/sy_prefer_mrs_conf/qvt_size_distribution"
prefer_time_dis    = "../conf_file/sy_prefer_mrs_conf/qvt_time_distribution"
module_show_name   = "../conf_file/qt_version_conf/module_show_name"

def readKeyValueFile( file_name ):
    res_dict = {}
    fp = open( file_name , 'r' )
    for line in fp.readlines()[1:]:
        infos = line[0:-1].split()
        res_dict[ infos[0] ] = infos[1]
    fp.close()
    return res_dict

    
def readKeyValueFileList( file_name ):
    res = []
    fp = open( file_name , 'r' )
    for line in fp.readlines()[1:]:
        infos = line[0:-1].split()
        res.append( infos )
    fp.close()
    return res
    

def readTableCan( ) :
    fp      = open( tb_file , 'r' )
    tb_tmp = [ line[0:-1].split( '\t' ) for line in fp.readlines() ]
    fp.close()
    
    tb_tmp.reverse()
    tb_has = {}
    tb_can = []
    for tc in tb_tmp :
        key = tc[0] + "|" + tc[1] + "|" +tc[2]
        if key not in tb_has :
            tb_can = tb_can + [ tc ]
            tb_has[ key ] = 1
    return tb_can


    
def readTopShowConf( ):
    fp       = open( top_page , 'r' )
    dict     = {}
    cur_conf = ""
    info_lst = []
    for line in fp.readlines():
        if len( line[0:-1] ) <= 0 :
            continue
        fields  = line[0:-1].split( '\t' )
        if fields[0] == "new_conf" :
            cur_conf = fields[1]
            info_lst = []
            dict[ cur_conf ] = [ "" , info_lst[ 0: ]  ]
            continue
        if len( fields ) > 1 :
            info_lst = info_lst + [ [ fields[0] , fields[1] ] ]
        if len( fields ) == 1:
            if fields[0] == "Total_ave" :
                dict[ cur_conf ][ 0 ] = "Total_ave"
            else:
                info_lst = info_lst + [ [ fields[0] , "" ] ]
        dict[ cur_conf ][ 1 ] = info_lst[ 0 : ]
    fp.close()
    return dict



def readVectorInfo( file_name ):
    fp      = open( file_name , 'r' )
    int_v   = [ string.atoi( line[0:-1] ) for line in fp.readlines() ]
    fp.close()
    return int_v
    

    
db_dict     = readKeyValueFile( db_file )
sum_dict    = readKeyValueFile( sum_file )
sy_prefer_mrs_type_dict = readKeyValueFile( sy_prefer_mrs_type_file )
resid_filter_dict = readKeyValueFile( resid_filter_qt_version )
from_resid_port_list = readKeyValueFileList( from_resid_port_file )
type_flag_type_dict = readKeyValueFile( type_flag_type_file )
module_show_name_dict = readKeyValueFile( module_show_name )

tb_can      = readTableCan( )
top_pg_dict = readTopShowConf( )

qvt_size_vec    = readVectorInfo( qvt_size_dis )
qvt_time_vec    = readVectorInfo( qvt_time_dis )
qvtm_time_vec   = readVectorInfo( qvtm_time_dis )
city_dist_vec   = readVectorInfo( city_dist_dis )
prefer_size_vec    = readVectorInfo( prefer_size_dis )
prefer_time_vec    = readVectorInfo( prefer_time_dis )

conn    = MySQLdb.connect( host = db_dict[ "server" ] , user = db_dict[ "user" ] ,passwd = db_dict[ "password" ] , db = db_dict[ "database" ] )
cur     = conn.cursor()




def refresh( ) :
    global db_dict
    global tb_can
    global top_pg_dict
    global qvt_size_vec
    global qvt_time_vec
    global qvtm_time_vec
    global city_dist_vec
    global prefer_size_vec
    global prefer_time_vec
    db_dict     = readKeyValueFile( db_file )
    sum_dict    = readKeyValueFile( sum_file )
    sy_prefer_mrs_type_dict = readKeyValueFile( sy_prefer_mrs_type_file )
    resid_filter_dict = readKeyValueFile( resid_filter_qt_version )
    from_resid_port_list = readKeyValueFileList( from_resid_port_file )
    type_flag_type_dict = readKeyValueFile( type_flag_type_file )
    module_show_name_dict = readKeyValueFile( module_show_name )
    tb_can      = readTableCan( )
    top_pg_dict = readTopShowConf( )
    qvt_size_vec    = readVectorInfo( qvt_size_dis )
    qvt_time_vec    = readVectorInfo( qvt_time_dis )
    qvtm_time_vec   = readVectorInfo( qvtm_time_dis )
    city_dist_vec   = readVectorInfo( city_dist_dis )
    prefer_size_vec    = readVectorInfo( prefer_size_dis )
    prefer_time_vec    = readVectorInfo( prefer_time_dis )

def reconnect():
    global conn
    global cur
    global db_dict
    global sum_dict
    cmd = "select qt from " + sum_dict[ "qt_version_time" ] + " limit 1";
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    if len( qres ) > 0 :
        print "reconnect ok"
    else :
        print "connect failed"


    
def from_resid_qt_version_to_port_name( from_info , resid , qt , version ):
    frqv = [ from_info , resid , qt , version ]
    for k in from_resid_port_list:
        key = ""
        for i in range( 4 ):
            if k[0][i] == "0" :
                key = key + "all,"
            else:
                key = key + frqv[i] + ","
        key = key[0:-1]
        if key == k[1]:
            return k[2]
    return "other_service"
    
    
    
def query_top_page_port_time_data_err_pv( st_time , ed_time ):
    global conn
    global cur
    global sum_dict
    cmd = "select from_info , resid , qt , version , time , query_num , err_num , avg_time , size , overtime_num , illegal_num from " + sum_dict[ "qt_version_time" ] ;
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    
    res = {}
    res[ "data_len" ]       = 0
    res[ "name_list" ]      = []
    res[ "day_range" ]      = []
    res[ "day_pv_num" ]     = []
    res[ "day_pv_percent" ] = []
    res[ "day_err_ratio" ]  = []
    res[ "day_avg_time" ]   = []
    res[ "day_avg_size" ]   = []
    res[ "day_ot_num_list" ]   = []
    res[ "day_ot_ratio_list" ] = []
    res[ "day_total_pv_list" ]     = []
    res[ "day_total_err_ratio" ]   = []
    res[ "day_total_avg_time" ]    = []
    res[ "day_total_avg_size" ]    = []
    res[ "day_week_pv_num" ]     = []
    res[ "day_week_pv_percent" ] = []
    res[ "day_week_err_ratio" ]  = []
    res[ "day_week_avg_time" ]   = []
    res[ "day_week_avg_size" ]   = []
    res[ "day_week_ot_num_list" ]   = []
    res[ "day_week_ot_ratio_list" ] = []
    res[ "day_week_total_pv_list" ]     = []
    res[ "day_week_total_err_ratio" ]   = []
    res[ "day_week_total_avg_time" ]    = []
    res[ "day_week_total_avg_size" ]    = []
    res[ "port_total_pv_num_list" ] = []
    res[ "port_total_pv_percent_list" ] = []
    res[ "port_total_avg_time_list" ] = []
    res[ "port_total_avg_size_list" ] = []
    res[ "port_total_avg_err_ratio_list" ] = []
    res[ "port_total_avg_ot_ratio_list" ] = []
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[4] , y[4] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
        
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 4 ] , qres[ -1 ][ 4 ] )
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    day_to_week_idx , week_day_count = day_zero_list[0:] , week_zero_list[0:]
    for i in range( len( day_to_week_idx ) ):
        day_to_week_idx[i] = common_method.UTChourToWeekIdx( (i+day_st)*24 ) - week_st
        week_day_count[ day_to_week_idx[i] ] += 1
        
    day_pv_dict , day_err_dict , day_time_dict , day_size_dict , day_illegal_dict , name_dict = {} , {} , {} , {} , {} , {}
    day_total_pv_list , day_total_err_list , day_total_illegal_list  = day_zero_list[0:], day_zero_list[0:], day_zero_list[0:]
    day_total_size_list , day_total_time_list, day_total_ot_num = day_zero_list[0:], day_zero_list[0:], day_zero_list[0:]
    total_pv_dict, total_err_dict, total_time_dict, total_size_dict, total_illegal_dict, total_ot_dict = {} , {} , {} , {} , {} , {}
    total_pv_sum = 0
    
    for qr in qres :
        key = from_resid_qt_version_to_port_name( qr[0] , qr[1] , qr[2] , qr[3] )
        if key not in name_dict:
            name_dict[ key ] = 1
            day_pv_dict[ key ]      = day_zero_list[0:]
            day_illegal_dict[ key ] = day_zero_list[0:]
            day_err_dict[ key ]     = day_zero_list[0:]
            day_time_dict[ key ]    = day_zero_list[0:]
            day_size_dict[ key ]    = day_zero_list[0:]
            total_pv_dict[ key ], total_err_dict[ key ], total_time_dict[ key ] = 0, 0, 0
            total_size_dict[ key ], total_illegal_dict[ key ], total_ot_dict[ key ] = 0, 0,0
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[4] ) )
        query_num, err_num, avg_time, total_size, ot_num = string.atoi(qr[5]), string.atoi(qr[6]), string.atof(qr[7]), string.atoi(qr[8]), string.atoi(qr[9])
        illegal_num = string.atoi( qr[10] )
        day_pv_dict[ key ][ day_idx - day_st ]   += query_num
        day_err_dict[ key ][ day_idx - day_st ]  += err_num
        day_time_dict[ key ][ day_idx - day_st ] += avg_time * query_num
        day_size_dict[ key ][ day_idx - day_st ] += total_size
        day_total_pv_list[ day_idx - day_st ]    += query_num
        day_total_err_list[ day_idx - day_st ]  += err_num
        day_total_time_list[ day_idx - day_st ] += avg_time * query_num
        day_total_size_list[ day_idx - day_st ] += total_size
        day_illegal_dict[ key ][ day_idx - day_st ] += illegal_num
        day_total_illegal_list[ day_idx - day_st ]  += illegal_num
        day_total_ot_num[ day_idx - day_st ]    += ot_num
        total_pv_dict[ key ]    += query_num
        total_err_dict[ key ]   += err_num
        total_time_dict[ key ]  += avg_time * query_num
        total_size_dict[ key ]  += total_size
        total_illegal_dict[key] += illegal_num
        total_ot_dict[ key ]    += ot_num
        total_pv_sum    += query_num
        
    for key in name_dict :
        res[ "name_list" ].append( key )
        res[ "day_pv_num" ].append( common_method.getListAdd( day_pv_dict[ key ] , day_illegal_dict[ key ] ) )
        res[ "day_pv_percent" ].append( common_method.getListDivide( 
                                    common_method.getListAdd( day_pv_dict[ key ] , day_illegal_dict[ key ] ) , 
                                    common_method.getListAdd( day_total_pv_list , day_total_illegal_list ) , 
                                    100.0 ) )
        res[ "day_err_ratio" ].append( common_method.getListDivide( day_err_dict[ key ] , day_pv_dict[ key ] , 100.0 ) )
        res[ "day_avg_time" ].append( common_method.getListDivide( day_time_dict[ key ] , day_pv_dict[ key ] , 1 ) )
        res[ "day_avg_size" ].append( common_method.getListDivide( day_size_dict[ key ] , day_pv_dict[ key ] , 1 ) )
        res[ "day_week_pv_num" ].append( common_method.getDayWeekAvg( res[ "day_pv_num" ][-1] , day_to_week_idx , week_day_count ) )
        res[ "day_week_pv_percent" ].append( common_method.getDayWeekAvg( res[ "day_pv_percent" ][-1] , day_to_week_idx , week_day_count ) )
        res[ "day_week_err_ratio" ].append( common_method.getDayWeekAvg( res[ "day_err_ratio" ][-1] , day_to_week_idx , week_day_count ) )
        res[ "day_week_avg_time" ].append( common_method.getDayWeekAvg( res[ "day_avg_time" ][-1] , day_to_week_idx , week_day_count ) )
        res[ "day_week_avg_size" ].append( common_method.getDayWeekAvg( res[ "day_avg_size" ][-1] , day_to_week_idx , week_day_count ) )
        res[ "port_total_pv_num_list" ].append( total_pv_dict[ key ] + total_illegal_dict[key] )
        res[ "port_total_pv_percent_list" ].append( (total_pv_dict[ key ] + total_illegal_dict[key]) * 100.0/total_pv_sum if total_pv_sum>0 else 0)
        res[ "port_total_avg_time_list" ].append( total_time_dict[ key ]/total_pv_dict[ key ] if total_pv_dict[ key ]>0 else 0 )
        res[ "port_total_avg_size_list" ].append( total_size_dict[ key ]/total_pv_dict[ key ] if total_pv_dict[ key ]>0 else 0 )
        res[ "port_total_avg_err_ratio_list" ].append( total_err_dict[ key ] * 100.0/total_pv_dict[ key ] if total_pv_dict[ key ]>0 else 0 )
        res[ "port_total_avg_ot_ratio_list" ].append( total_ot_dict[ key ]*100.0/total_pv_dict[ key ] if total_pv_dict[ key ]>0 else 0 )
    res[ "day_total_pv_list" ]   = common_method.getListAdd( day_total_pv_list , day_total_illegal_list )
    res[ "day_total_err_ratio" ] = common_method.getListDivide( day_total_err_list , day_total_pv_list , 100.0 )
    res[ "day_total_avg_time" ]  = common_method.getListDivide( day_total_time_list , day_total_pv_list , 1.0 )
    res[ "day_total_avg_size" ]  = common_method.getListDivide( day_total_size_list , day_total_pv_list , 1.0 )
    res[ "day_week_total_pv_list" ]     = common_method.getDayWeekAvg( res[ "day_total_pv_list" ], day_to_week_idx , week_day_count )
    res[ "day_week_total_err_ratio" ]   = common_method.getDayWeekAvg( res[ "day_total_err_ratio" ], day_to_week_idx , week_day_count )
    res[ "day_week_total_avg_time" ]    = common_method.getDayWeekAvg( res[ "day_total_avg_time" ], day_to_week_idx , week_day_count )
    res[ "day_week_total_avg_size" ]    = common_method.getDayWeekAvg( res[ "day_total_avg_size" ], day_to_week_idx , week_day_count )
    res[ "day_ot_num_list" ] = day_total_ot_num
    res[ "day_ot_ratio_list" ] = common_method.getListDivide( day_total_ot_num , day_total_pv_list , 100.0 )
    res[ "day_week_ot_num_list" ] = common_method.getDayWeekAvg( res[ "day_ot_num_list" ] , day_to_week_idx , week_day_count ) 
    res[ "day_week_ot_ratio_list" ] = common_method.getDayWeekAvg( res[ "day_ot_ratio_list" ] , day_to_week_idx , week_day_count ) 
    return res 
    

    
    
    
    
    
    
def deal_kes_stat_info_dict(dict):
    navi_kes_online_navi_cnt = 0
    navi_kes_online_yaw_cnt = 0
    navi_kes_offline_navi_cnt = 0
    navi_kes_offline_yaw_cnt = 0
    navi_kes_uv = 0
    navi_kes_yaw_uv = 0
    total = 0
    total_err_ratio = 0
    routecalc_count_net = 0
    routecalc_count_local = 0
    if "routecalc_count_net" in dict:
        routecalc_count_net = string.atof(dict["routecalc_count_net"])
    if "routecalc_count_local" in dict:
        routecalc_count_local = string.atof(dict["routecalc_count_local"])
    if "navi_kes_online_navi_cnt" in dict:
        navi_kes_online_navi_cnt = string.atof(dict["navi_kes_online_navi_cnt"])
    if "navi_kes_online_yaw_cnt" in dict:
        navi_kes_online_yaw_cnt = string.atof(dict["navi_kes_online_yaw_cnt"])
    if "navi_kes_offline_navi_cnt" in dict:
        navi_kes_offline_navi_cnt = string.atof(dict["navi_kes_offline_navi_cnt"])
    if "navi_kes_offline_yaw_cnt" in dict:
        navi_kes_offline_yaw_cnt = string.atof(dict["navi_kes_offline_yaw_cnt"])
    if "navi_kes_uv" in dict:
        navi_kes_uv = string.atof(dict["navi_kes_uv"])
    if "navi_kes_yaw_uv" in dict:
        navi_kes_yaw_uv = string.atof(dict["navi_kes_yaw_uv"])
    if "total" in dict:
        total = string.atof(dict["total"])
    if "total_err_ratio" in dict:
        total_err_ratio = string.atof(dict["total_err_ratio"])
    routecalc_count = routecalc_count_net + routecalc_count_local
    navi_kes_online_navi_yaw_cnt = navi_kes_online_navi_cnt + navi_kes_online_yaw_cnt
    navi_kes_offline_navi_yaw_cnt = navi_kes_offline_navi_cnt + navi_kes_offline_yaw_cnt
    res = {}
    res["baidu_err_ratio"] = total_err_ratio if total_err_ratio>0 else -9999999
    res["routecalc_count_net_percent"] = routecalc_count_net*100.0/routecalc_count if routecalc_count>0 else -9999999
    res["routecalc_count_local_percent"] = routecalc_count_local*100.0/routecalc_count if routecalc_count>0 else -9999999
    res["navi_kes_online_yaw_ratio"] = navi_kes_online_yaw_cnt*100.0/navi_kes_online_navi_yaw_cnt if navi_kes_online_navi_yaw_cnt>0 else -9999999
    res["navi_kes_offline_yaw_ratio"] = navi_kes_offline_yaw_cnt*100.0/navi_kes_offline_navi_yaw_cnt if navi_kes_offline_navi_yaw_cnt>0 else -9999999
    res["navi_kes_yaw_uv_ratio"] = navi_kes_yaw_uv*100.0/navi_kes_uv if navi_kes_uv>0 else -9999999
    res["time_compare_ratio"] = total if total>0 else -9999999
    return res

def get_week_compare_ratio(cur_week, pre_week):    
    if cur_week<0 or pre_week<=0:
        return -9999999
    return (cur_week - pre_week ) * 100 / pre_week
    
def get_week_compare_list(dict_list, key_name):
    res = []
    week_day_cnt=[0,0,0]
    week_sum_list=[0,0,0]
    for i in range(len(dict_list)):
        if dict_list[i][key_name] < 0:
            continue
        week_day_cnt[i/7] += 1
        week_sum_list[i/7] += dict_list[i][key_name]
    week_avg_list = [week_sum_list[i]/week_day_cnt[i] if week_day_cnt[i]>0 else -9999999 for i in range(3)]
    res.append(week_avg_list[-1])
    res.append(get_week_compare_ratio(week_avg_list[-1], week_avg_list[-2]))
    res.append(week_avg_list[-2])
    res.append(get_week_compare_ratio(week_avg_list[-2], week_avg_list[-3]))
    res.append(week_avg_list[-3])
    return res
    
def query_kes_stat_info():
    global conn
    global cur
    global sum_dict
    date_from   = common_method.getDayAgoStr( -21 )
    date_to     = common_method.getDayAgoStr( -1 )
    hour_from   = common_method.dateToHour( date_from )
    hour_to     = common_method.dateToHour( date_to )
    day_zero_list = [0 for i in range(21)]
    week_zero_list = [0 for i in range(3)]
    
    cmd = "select kes_name, time, kes_value from kes_stat_info" ;
    cmd = cmd + " where time >= " + str( hour_from ) + " and time <= " + str( hour_to )   
    res = {}
    res[ "data_len" ] = 0
    res[ "date_list" ] = [common_method.getDayAgoStr( -1 * i ) for i in range(1,22)]
    res["baidu_err_ratio"] = [-9999999 for i in range(5)]
    res["routecalc_count_net_percent"] = [-9999999 for i in range(5)]
    res["routecalc_count_local_percent"] = [-9999999 for i in range(5)]
    res["navi_kes_online_yaw_ratio"] = [-9999999 for i in range(5)]
    res["navi_kes_offline_yaw_ratio"] = [-9999999 for i in range(5)]
    res["navi_kes_yaw_uv_ratio"] = [-9999999 for i in range(5)]
    res["time_compare_ratio"] = [-9999999 for i in range(5)]
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
        
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
    
    dict_list = [{} for i in day_zero_list]
    for qr in qres:
        day_idx = (string.atoi(qr[1]) - hour_from) / 24
        dict_list[day_idx][qr[0]] = qr[2]
    dict_list = [deal_kes_stat_info_dict(dict) for dict in dict_list]    
    res["baidu_err_ratio"] = get_week_compare_list(dict_list, "baidu_err_ratio")
    res["routecalc_count_net_percent"] = get_week_compare_list(dict_list, "routecalc_count_net_percent")
    res["routecalc_count_local_percent"] = get_week_compare_list(dict_list, "routecalc_count_local_percent")
    res["navi_kes_online_yaw_ratio"] = get_week_compare_list(dict_list, "navi_kes_online_yaw_ratio")
    res["navi_kes_offline_yaw_ratio"] = get_week_compare_list(dict_list, "navi_kes_offline_yaw_ratio")
    res["navi_kes_yaw_uv_ratio"] = get_week_compare_list(dict_list, "navi_kes_yaw_uv_ratio")
    res["time_compare_ratio"] = get_week_compare_list(dict_list, "time_compare_ratio")
    return res 

    

#hour
#select city , time , query_str , dist_vector from city_time_num_frequence where time <=b and time >=a  
def query_city_query_num( st_time , ed_time , search_model ):
    global conn
    global cur
    global sum_dict

    data_vector = city_dist_vec[ 0 : ]
    res = {}
    res[ "name_list" ]  = []
    res[ "data_len" ]   = 0
    res[ "hour_range" ]     = []
    res[ "hour_list" ]      = []
    res[ "hour_ratio_list" ]= []
    res[ "day_range" ]      = []
    res[ "day_list" ]       = []
    res[ "day_ratio_list" ] = []
    res[ "week_range" ]     = []
    res[ "week_list" ]      = []
    res[ "week_ratio_list" ]= []
    
    res[ "data_vector" ]    = data_vector
    res[ "data_div_list" ]  = []
    cmd = "select city , time , query_str , dist_vector from " + sum_dict[ "city_time_num_frequence" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[1] , y[1] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    
    if len( qres ) <=0 :
            return res
            
    res[ "data_len" ] = len( qres )
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 1 ] , string.atoi( qres[ -1 ][ 1 ] ) + 23 )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    dist_zero_list  = [ 0 for i in range( 1 + len( data_vector ) ) ]
    name_dict       = {}
    hour_dict       = {}
    day_dict        = {}
    week_dict       = {}
    data_div_dict   = {}
    hour_all_d  = hour_zero_list[ 0 : ]
    day_all_d   = day_zero_list[ 0 : ]
    week_all_d  = week_zero_list[ 0 : ]
    for qr in qres :
        key = qr[0]
        if key not in name_dict:
            hour_dict[ key ]    = hour_zero_list[ 0 : ]
            day_dict[ key ]     = day_zero_list[ 0 : ]
            week_dict[ key ]    = week_zero_list[ 0 : ]
            data_div_dict[ key ]= dist_zero_list[ 0 : ]
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[1] ) )
        query_list  = [ string.atoi( ql ) for ql in qr[2].split( '|' ) ]
        for h in range( 24 ):
            hour_dict[ key ][ hour_idx - hour_st + h ] += query_list[ h ]
            hour_all_d[ hour_idx - hour_st + h ] += query_list[ h ]
        day_query_n = common_method.getListSum( query_list )
        day_dict[ key ][ day_idx - day_st ] += day_query_n
        day_all_d[ day_idx - day_st ] += day_query_n
        week_dict[ key ][ week_idx - week_st ]  += day_query_n
        week_all_d[ week_idx - week_st ]    += day_query_n
        one_div_list        = dist_zero_list[ 0 : ]
        if len( qr[3] ) >= len( dist_zero_list ) :
            one_div_list    = [ string.atoi( qrr ) for qrr in qr[3].split( '|' ) ] 
        data_div_dict[ key ]= common_method.getListAdd( one_div_list , data_div_dict[ key ] )
        name_dict[ key ] = 1
    all_city_list = []
    for key in name_dict:
        one_city = []
        one_city.append( key )
        one_city.append( hour_dict[ key ] )
        one_city.append( common_method.getListDivide( hour_dict[ key ] , hour_all_d , 100.0 ) )
        one_city.append( day_dict[ key ] )
        one_city.append( common_method.getListDivide( day_dict[ key ] , day_all_d , 100.0 ) )
        one_city.append( week_dict[ key ] )
        one_city.append( common_method.getListDivide( week_dict[ key ] , week_all_d , 100.0 ) )
        query_num = common_method.getListSum( data_div_dict[ key ] )
        one_city.append( [ ddd * 100.0 / query_num  if query_num > 0 else  0 for ddd in data_div_dict[ key ] ] )
        all_city_list.append( one_city )
    #print all_city_list
    if search_model == 'DAY' :
        all_city_list.sort( lambda x,y:cmp( x[3][-1] , y[3][-1] ) , reverse=True )
    elif search_model == 'WEEK' :
        all_city_list.sort( lambda x,y:cmp( x[5][-1] , y[5][-1] ) , reverse=True )
    for i in range( 20 ):
        res[ "name_list" ].append( all_city_list[i][0] )
        res[ "hour_list" ].append( all_city_list[i][1] )
        res[ "hour_ratio_list" ].append( all_city_list[i][2] )
        res[ "day_list" ].append( all_city_list[i][3] )
        res[ "day_ratio_list" ].append( all_city_list[i][4] )
        res[ "week_list" ] .append( all_city_list[i][5] )
        res[ "week_ratio_list" ].append( all_city_list[i][6] )
        res[ "data_div_list" ].append( all_city_list[i][7] )
    return res
    

def query_city_query_num_for_port( st_time , ed_time , search_model , port_name ):
    global conn
    global cur
    global sum_dict
    data_vector = city_dist_vec[ 0 : ]
    res = {}
    res[ "name_list" ]  = []
    res[ "data_len" ]   = 0
    res[ "hour_range" ]     = []
    res[ "hour_list" ]      = []
    res[ "hour_ratio_list" ]= []
    res[ "day_range" ]      = []
    res[ "day_list" ]       = []
    res[ "day_ratio_list" ] = []
    res[ "week_range" ]     = []
    res[ "week_list" ]      = []
    res[ "week_ratio_list" ]= []
    res[ "data_vector" ]    = data_vector
    res[ "data_div_list" ]  = []
    cmd = "select city , time , query_str , dist_vector , from_info , resid , qt , version from " + sum_dict[ "port_city_time_num" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[1] , y[1] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    
    if len( qres ) <=0 :
            return res
            
    res[ "data_len" ] = len( qres )
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 1 ] , string.atoi( qres[ -1 ][ 1 ] ) + 23 )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    dist_zero_list  = [ 0 for i in range( 1 + len( data_vector ) ) ]
    name_dict       = {}
    hour_dict       = {}
    day_dict        = {}
    week_dict       = {}
    data_div_dict   = {}
    hour_all_d  = hour_zero_list[ 0 : ]
    day_all_d   = day_zero_list[ 0 : ]
    week_all_d  = week_zero_list[ 0 : ]
    for qr in qres :
        key = from_resid_qt_version_to_port_name( qr[4] , qr[5] , qr[6] , qr[7] )
        if key != port_name:
            continue
        key = qr[0]
        if key not in name_dict:
            hour_dict[ key ]    = hour_zero_list[ 0 : ]
            day_dict[ key ]     = day_zero_list[ 0 : ]
            week_dict[ key ]    = week_zero_list[ 0 : ]
            data_div_dict[ key ]= dist_zero_list[ 0 : ]
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[1] ) )
        query_list  = [ string.atoi( ql ) for ql in qr[2].split( '|' ) ]
        for h in range( 24 ):
            hour_dict[ key ][ hour_idx - hour_st + h ] += query_list[ h ]
            hour_all_d[ hour_idx - hour_st + h ] += query_list[ h ]
        day_query_n = common_method.getListSum( query_list )
        day_dict[ key ][ day_idx - day_st ] += day_query_n
        day_all_d[ day_idx - day_st ] += day_query_n
        week_dict[ key ][ week_idx - week_st ]  += day_query_n
        week_all_d[ week_idx - week_st ]    += day_query_n
        one_div_list        = dist_zero_list[ 0 : ]
        if len( qr[3] ) >= len( dist_zero_list ) :
            one_div_list    = [ string.atoi( qrr ) for qrr in qr[3].split( '|' ) ] 
        data_div_dict[ key ]= common_method.getListAdd( one_div_list , data_div_dict[ key ] )
        name_dict[ key ] = 1
    all_city_list = []
    for key in name_dict:
        one_city = []
        one_city.append( key )
        one_city.append( hour_dict[ key ] )
        one_city.append( common_method.getListDivide( hour_dict[ key ] , hour_all_d , 100.0 ) )
        one_city.append( day_dict[ key ] )
        one_city.append( common_method.getListDivide( day_dict[ key ] , day_all_d , 100.0 ) )
        one_city.append( week_dict[ key ] )
        one_city.append( common_method.getListDivide( week_dict[ key ] , week_all_d , 100.0 ) )
        query_num = common_method.getListSum( data_div_dict[ key ] )
        one_city.append( [ ddd * 100.0 / query_num  if query_num > 0 else  0 for ddd in data_div_dict[ key ] ] )
        all_city_list.append( one_city )
    if len( name_dict ) <=0 :
        res[ "data_len" ] = 0
        return res
    #print all_city_list
    if search_model == 'DAY' :
        all_city_list.sort( lambda x,y:cmp( x[3][-1] , y[3][-1] ) , reverse=True )
    elif search_model == 'WEEK' :
        all_city_list.sort( lambda x,y:cmp( x[5][-1] , y[5][-1] ) , reverse=True )
    for i in range( 20 ):
        res[ "name_list" ].append( all_city_list[i][0] )
        res[ "hour_list" ].append( all_city_list[i][1] )
        res[ "hour_ratio_list" ].append( all_city_list[i][2] )
        res[ "day_list" ].append( all_city_list[i][3] )
        res[ "day_ratio_list" ].append( all_city_list[i][4] )
        res[ "week_list" ] .append( all_city_list[i][5] )
        res[ "week_ratio_list" ].append( all_city_list[i][6] )
        res[ "data_div_list" ].append( all_city_list[i][7] )
    return res    
    
    
def query_top_page_city_query_num(  ) :
    date_from,date_to   = common_method.get_pass_weeks( 4 )
    hour_from,hour_to   = common_method.dateToHour( date_from ) , common_method.dateToHour( date_to ) + 23
    dict = query_city_query_num( hour_from , hour_to , "DAY" )
    dict.pop( "hour_list" )
    dict.pop( "hour_ratio_list" )
    dict.pop( "data_div_list" )
    dict[ "day_list" ]       = [ ddl[-7:] for ddl in dict[ "day_list" ]]
    dict[ "day_ratio_list" ] = [ ddl[-7:] for ddl in dict[ "day_ratio_list" ]]
    dict[ "day_range" ][0]   = dict[ "day_range" ][1] - 24 * 6
    return dict
    
    
    
    
#hour
#select city , time , query_str from city_time_num_frequence where time <=b and time >=a    
def query_city_hf( st_time , ed_time , city ):
    global conn
    global cur
    global sum_dict

    cmd = "select city , trip from " + sum_dict[ "city_time_num_frequence" ] 
    cmd = cmd + " where city=\"" + city + "\" and time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    print cmd
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
        #print qres
        
    if len( qres ) <=0 :
        return []
            
    pp_dict = {}
    for qr in qres:
        for sen in qr[1].split( '|' ):
            s_e_n   = sen.split( ',' )
            key = s_e_n[0] + ',' + s_e_n[1]
            if key not in pp_dict:
                pp_dict[ key ] = 0
            pp_dict[ key ] = pp_dict[ key ] + string.atoi( s_e_n[2] )
    pp_list = []
    for key in pp_dict:
        pp_list.append( [ key , pp_dict[ key ] ] )
    pp_list.sort( lambda x,y:cmp( x[1] , y[1] ) , reverse=True )
    #500 100000
    pp_list = pp_list[ 0 : 1000 ]
    pp_list = [ [ string.atoi( ppll ) for ppll in ppl[0].split( ',' ) ] + [ ppl[-1] ] for ppl in pp_list ]
    pp_list = [ [ ( ppl[0]/100000 ) * 500 , ( ppl[0]%100000 ) * 500 , ( ppl[1]/100000 ) * 500 , ( ppl[1]%100000 ) * 500 , ppl[-1] ] for ppl in pp_list ]
    pp_list = [ [ ppl[0] + 250 , ppl[1] + 250 , ppl[2] + 250 , ppl[3] + 250 , ppl[-1] ] for ppl in pp_list ]
    pp_list = [ [ util.coordtrans( "bd09mc","bd09ll", ppl[0] , ppl[1] ) , util.coordtrans( "bd09mc","bd09ll", ppl[2] , ppl[3] ) , ppl[-1] ] for ppl in pp_list ]
    pp_list = [ [ ppl[0][0] , ppl[0][1] , ppl[1][0] , ppl[1][1] , ppl[-1] ] for ppl in pp_list ]    
    return pp_list

    
    
    
   
def query_port_pv_query( st_time , ed_time ):
    global conn
    global cur
    global sum_dict

    res = {}
    res[ "data_len" ]           = 0
    res[ "name_list" ]          = []
    
    res[ "hour_range" ]         = []
    res[ "hour_num_list" ]      = []
    res[ "hour_num_ratio" ]     = []
    res[ "hour_total_num" ]     = []
    
    res[ "day_range" ]         = []
    res[ "day_num_list" ]      = []
    res[ "day_num_ratio" ]     = []
    res[ "day_total_num" ]     = []
    res[ "day_week_num_list" ]  = []
    res[ "day_week_num_ratio" ] = []
    res[ "day_week_total_num" ] = []
    
    res[ "week_range" ]         = []
    res[ "week_num_list" ]      = []
    res[ "week_num_ratio" ]     = []
    res[ "week_total_num" ]     = []
    res[ "week_day_num_list" ]  = []
    res[ "week_day_num_ratio" ] = []
    res[ "week_day_total_num" ] = []
    res[ "all_num_list" ]       = []

    cmd = "select from_info , resid , qt , version , time , query_num , illegal_num from " + sum_dict[ "qt_version_time" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[4] , y[4] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    #print qres
        
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
        
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 4 ] , qres[ -1 ][ 4 ] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    day_to_week_idx , week_day_count = day_zero_list[0:] , week_zero_list[0:]
    for i in range( len( day_to_week_idx ) ):
        day_to_week_idx[i] = common_method.UTChourToWeekIdx( (i+day_st)*24 ) - week_st
        week_day_count[ day_to_week_idx[i] ] += 1
        
    name_dict , hour_num_dict , day_num_dict , week_num_dict , all_num_dict = {} , {} , {} , {} ,{}
    hour_num_all , day_num_all , week_num_all = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    hour_total_num , day_total_num , week_total_num = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    
    for qr in qres :
        key = from_resid_qt_version_to_port_name( qr[0] , qr[1] , qr[2] , qr[3] )
        if key not in name_dict:
            name_dict[ key ] = 1
            hour_num_dict[ key ]    = hour_zero_list[0:]
            day_num_dict[ key ]     = day_zero_list[0:]
            week_num_dict[ key ]    = week_zero_list[0:]
            all_num_dict[ key ]     = 0
        query_num = string.atoi( qr[5] ) + string.atoi( qr[6] )
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[4] ) )
        hour_num_dict[ key ][ hour_idx - hour_st ]  += query_num
        day_num_dict[ key ][ day_idx - day_st ]     += query_num
        week_num_dict[ key ][ week_idx - week_st ]  += query_num
        hour_num_all[ hour_idx - hour_st ]  += query_num
        day_num_all[ day_idx - day_st ]     += query_num
        week_num_all[ week_idx - week_st ]  += query_num
        hour_total_num[ hour_idx - hour_st ]  += query_num
        day_total_num[ day_idx - day_st ]     += query_num
        week_total_num[ week_idx - week_st ]  += query_num
        all_num_dict[ key ] += query_num
    for key  in name_dict:
        res[ "name_list" ].append( key )
        res[ "hour_num_list" ].append( hour_num_dict[ key ] )
        res[ "hour_num_ratio" ].append( common_method.getListDivide( hour_num_dict[ key ] , hour_num_all , 100.0 ) )
        res[ "day_num_list" ].append( day_num_dict[ key ] )
        res[ "day_num_ratio" ].append( common_method.getListDivide( day_num_dict[ key ] , day_num_all , 100.0 ) )
        res[ "week_num_list" ].append( week_num_dict[ key ] )
        res[ "week_num_ratio" ].append( common_method.getListDivide( week_num_dict[ key ] , week_num_all , 100.0 ) )
        res[ "all_num_list" ].append( all_num_dict[ key ] )
        res[ "day_week_num_list" ].append( common_method.getDayWeekAvg( res[ "day_num_list" ][-1] , day_to_week_idx , week_day_count ) )
        res[ "day_week_num_ratio" ].append( common_method.getDayWeekAvg( res[ "day_num_ratio" ][-1] , day_to_week_idx , week_day_count ) )
        res[ "week_day_num_list" ].append( common_method.getWeekDayAvg( res[ "day_num_list" ][-1] , day_to_week_idx , week_day_count ) )
        res[ "week_day_num_ratio" ].append( common_method.getWeekDayAvg( res[ "day_num_ratio" ][-1] , day_to_week_idx , week_day_count ) )
    res["hour_total_num"] = hour_total_num
    res["day_total_num"] = day_total_num
    res["week_total_num"] = week_total_num
    res[ "day_week_total_num" ] = common_method.getDayWeekAvg( day_total_num , day_to_week_idx , week_day_count )
    res[ "week_day_total_num" ] = common_method.getWeekDayAvg( day_total_num , day_to_week_idx , week_day_count )
    return res    


def query_port2version_pv_query( st_time , ed_time , port_name ):
    global conn
    global cur
    global sum_dict

    res = {}
    res[ "data_len" ]           = 0
    res[ "name_list" ]          = []
    
    res[ "hour_range" ]         = []
    res[ "hour_num_list" ]      = []
    res[ "hour_num_ratio" ]     = []
    
    res[ "day_range" ]         = []
    res[ "day_num_list" ]      = []
    res[ "day_num_ratio" ]     = []
    
    res[ "week_range" ]         = []
    res[ "week_num_list" ]      = []
    res[ "week_num_ratio" ]     = []
    
    res[ "all_num_list" ]       = []

    cmd = "select from_info , resid , qt , version , time , query_num , illegal_num from " + sum_dict[ "qt_version_time" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[4] , y[4] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    #print qres
        
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
        
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 4 ] , qres[ -1 ][ 4 ] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    
    name_dict , hour_num_dict , day_num_dict , week_num_dict , all_num_dict = {} , {} , {} , {} ,{}
    hour_num_all , day_num_all , week_num_all = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    
    for qr in qres :
        key = from_resid_qt_version_to_port_name( qr[0] , qr[1] , qr[2] , qr[3] )
        if key != port_name :
            continue
        key = qr[2] + ',' + qr[3]
        if key not in name_dict:
            name_dict[ key ] = 1
            hour_num_dict[ key ]    = hour_zero_list[0:]
            day_num_dict[ key ]     = day_zero_list[0:]
            week_num_dict[ key ]    = week_zero_list[0:]
            all_num_dict[ key ]     = 0
        query_num = string.atoi( qr[5] ) + string.atoi( qr[6] )
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[4] ) )
        hour_num_dict[ key ][ hour_idx - hour_st ]  += query_num
        day_num_dict[ key ][ day_idx - day_st ]     += query_num
        week_num_dict[ key ][ week_idx - week_st ]  += query_num
        hour_num_all[ hour_idx - hour_st ]  += query_num
        day_num_all[ day_idx - day_st ]     += query_num
        week_num_all[ week_idx - week_st ]  += query_num
        all_num_dict[ key ] += query_num
    for key  in name_dict:
        res[ "name_list" ].append( key )
        res[ "hour_num_list" ].append( hour_num_dict[ key ] )
        res[ "hour_num_ratio" ].append( common_method.getListDivide( hour_num_dict[ key ] , hour_num_all , 100.0 ) )
        res[ "day_num_list" ].append( day_num_dict[ key ] )
        res[ "day_num_ratio" ].append( common_method.getListDivide( day_num_dict[ key ] , day_num_all , 100.0 ) )
        res[ "week_num_list" ].append( week_num_dict[ key ] )
        res[ "week_num_ratio" ].append( common_method.getListDivide( week_num_dict[ key ] , week_num_all , 100.0 ) )
        res[ "all_num_list" ].append( all_num_dict[ key ] )
    return res        
    
    
def query_port_error_query( st_time , ed_time ):
    global conn
    global cur
    global sum_dict

    res = {}
    res[ "data_len" ]           = 0
    res[ "name_list" ]          = []
    
    res[ "hour_range" ]         = []
    res[ "hour_err_ratio" ]     = []
    res[ "hour_err_percent" ]   = []
    res[ "hour_total_ratio" ]   = []
    
    res[ "day_range" ]         = []
    res[ "day_err_ratio" ]     = []
    res[ "day_err_percent" ]   = []
    res[ "day_total_ratio" ]   = []
    res[ "day_week_err_ratio" ]     = []
    res[ "day_week_err_percent" ]   = []
    res[ "day_week_total_ratio" ]   = []
    
    res[ "week_range" ]         = []
    res[ "week_err_ratio" ]     = []
    res[ "week_err_percent" ]   = []
    res[ "week_total_ratio" ]   = []
    res[ "week_day_err_ratio" ]     = []
    res[ "week_day_err_percent" ]   = []
    res[ "week_day_total_ratio" ]   = []
    res[ "all_err_ratio" ]      = []
    res[ "all_err_percent" ]    = []

    cmd = "select from_info , resid , qt , version , time , query_num , err_num from " + sum_dict[ "qt_version_time" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[4] , y[4] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    #print qres
        
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
        
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 4 ] , qres[ -1 ][ 4 ] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    day_to_week_idx , week_day_count = day_zero_list[0:] , week_zero_list[0:]
    for i in range( len( day_to_week_idx ) ):
        day_to_week_idx[i] = common_method.UTChourToWeekIdx( (i+day_st)*24 ) - week_st
        week_day_count[ day_to_week_idx[i] ] += 1
    
    name_dict , hour_num_dict , day_num_dict , week_num_dict , all_num_dict = {} , {} , {} , {} ,{}
    hour_err_dict , day_err_dict , week_err_dict , all_err_dict = {} , {} , {} , {}
    hour_err_all , day_err_all , week_err_all = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    hour_pv_all , day_pv_all , week_pv_all = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    all_err_all = 0
    
    for qr in qres :
        key = from_resid_qt_version_to_port_name( qr[0] , qr[1] , qr[2] , qr[3] )
        if key not in name_dict:
            name_dict[ key ] = 1
            hour_num_dict[ key ] , hour_err_dict[ key ]    = hour_zero_list[0:] , hour_zero_list[0:]
            day_num_dict[ key ] , day_err_dict[ key ]      = day_zero_list[0:] , day_zero_list[0:]
            week_num_dict[ key ] , week_err_dict[ key ]    = week_zero_list[0:] , week_zero_list[0:]
            all_num_dict[ key ] , all_err_dict[ key ]      = 0 , 0
        query_num , err_num = string.atoi( qr[5] ) , string.atoi( qr[6] )
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[4] ) )
        hour_num_dict[ key ][ hour_idx - hour_st ]  += query_num
        day_num_dict[ key ][ day_idx - day_st ]     += query_num
        week_num_dict[ key ][ week_idx - week_st ]  += query_num
        all_num_dict[ key ] += query_num
        hour_err_dict[ key ][ hour_idx - hour_st ]  += err_num
        day_err_dict[ key ][ day_idx - day_st ]     += err_num
        week_err_dict[ key ][ week_idx - week_st ]  += err_num
        hour_pv_all[ hour_idx - hour_st ]  += query_num
        day_pv_all[ day_idx - day_st ]     += query_num
        week_pv_all[ week_idx - week_st ]  += query_num
        hour_err_all[ hour_idx - hour_st ]  += err_num
        day_err_all[ day_idx - day_st ]     += err_num
        week_err_all[ week_idx - week_st ]  += err_num
        all_err_dict[ key ] += err_num
        all_err_all += err_num

    for key  in name_dict:
        res[ "name_list" ].append( key )
        res[ "hour_err_ratio" ].append( common_method.getListDivide( hour_err_dict[ key ] , hour_num_dict[ key ] , 100.0 ) )
        res[ "hour_err_percent" ].append( common_method.getListDivide( hour_err_dict[ key ] , hour_err_all , 100.0 ) )
        res[ "day_err_ratio" ].append( common_method.getListDivide( day_err_dict[ key ] , day_num_dict[ key ] , 100.0 ) )
        res[ "day_err_percent" ].append( common_method.getListDivide( day_err_dict[ key ] , day_err_all , 100.0 ) )
        res[ "week_err_ratio" ].append( common_method.getListDivide( week_err_dict[ key ] , week_num_dict[ key ] , 100.0 ) )
        res[ "week_err_percent" ].append( common_method.getListDivide( week_err_dict[ key ] , week_err_all , 100.0 ) )
        res[ "all_err_ratio" ].append( all_err_dict[ key ] * 100.0 / all_num_dict[ key ] if all_num_dict[ key ] > 0 else 0 )
        res[ "all_err_percent" ].append( all_err_dict[ key ] * 100.0 / all_err_all if all_err_all > 0 else 0 )
        res[ "day_week_err_ratio" ].append( common_method.getDayWeekAvg( res[ "day_err_ratio" ][-1] , day_to_week_idx , week_day_count ) )
        res[ "day_week_err_percent" ].append( common_method.getDayWeekAvg( res[ "day_err_percent" ][-1] , day_to_week_idx , week_day_count ) )
        res[ "week_day_err_ratio" ].append( common_method.getWeekDayAvg( res[ "day_err_ratio" ][-1] , day_to_week_idx , week_day_count ) )
        res[ "week_day_err_percent" ].append( common_method.getWeekDayAvg( res[ "day_err_percent" ][-1] , day_to_week_idx , week_day_count ) )
    res["hour_total_ratio"] = common_method.getListDivide( hour_err_all , hour_pv_all , 100.0 )
    res["day_total_ratio"] = common_method.getListDivide( day_err_all , day_pv_all , 100.0 )
    res["week_total_ratio"] = common_method.getListDivide( week_err_all , week_pv_all , 100.0 )
    res[ "day_week_total_ratio" ] = common_method.getDayWeekAvg( res["day_total_ratio"] , day_to_week_idx , week_day_count )
    res[ "week_day_total_ratio" ] = common_method.getWeekDayAvg( res["day_total_ratio"] , day_to_week_idx , week_day_count )
    return res    


def query_port2version_error_query( st_time , ed_time , port_name ):
    global conn
    global cur
    global sum_dict

    res = {}
    res[ "data_len" ]           = 0
    res[ "name_list" ]          = []
    
    res[ "hour_range" ]         = []
    res[ "hour_err_ratio" ]     = []
    res[ "hour_err_percent" ]   = []
    
    res[ "day_range" ]         = []
    res[ "day_err_ratio" ]     = []
    res[ "day_err_percent" ]   = []
    
    res[ "week_range" ]         = []
    res[ "week_err_ratio" ]     = []
    res[ "week_err_percent" ]   = []

    res[ "all_err_ratio" ]      = []
    res[ "all_err_percent" ]    = []

    cmd = "select from_info , resid , qt , version , time , query_num , err_num from " + sum_dict[ "qt_version_time" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[4] , y[4] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    #print qres
        
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
        
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 4 ] , qres[ -1 ][ 4 ] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    
    name_dict , hour_num_dict , day_num_dict , week_num_dict , all_num_dict = {} , {} , {} , {} ,{}
    hour_err_dict , day_err_dict , week_err_dict , all_err_dict = {} , {} , {} , {}
    hour_err_all , day_err_all , week_err_all = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    all_err_all = 0
    
    for qr in qres :
        key = from_resid_qt_version_to_port_name( qr[0] , qr[1] , qr[2] , qr[3] )
        if key != port_name :
            continue
        key = qr[2] + ',' + qr[3]
        if key not in name_dict:
            name_dict[ key ] = 1
            hour_num_dict[ key ] , hour_err_dict[ key ]    = hour_zero_list[0:] , hour_zero_list[0:]
            day_num_dict[ key ] , day_err_dict[ key ]      = day_zero_list[0:] , day_zero_list[0:]
            week_num_dict[ key ] , week_err_dict[ key ]    = week_zero_list[0:] , week_zero_list[0:]
            all_num_dict[ key ] , all_err_dict[ key ]      = 0 , 0
        query_num , err_num = string.atoi( qr[5] ) , string.atoi( qr[6] )
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[4] ) )
        hour_num_dict[ key ][ hour_idx - hour_st ]  += query_num
        day_num_dict[ key ][ day_idx - day_st ]     += query_num
        week_num_dict[ key ][ week_idx - week_st ]  += query_num
        all_num_dict[ key ] += query_num
        hour_err_dict[ key ][ hour_idx - hour_st ]  += err_num
        day_err_dict[ key ][ day_idx - day_st ]     += err_num
        week_err_dict[ key ][ week_idx - week_st ]  += err_num
        hour_err_all[ hour_idx - hour_st ]  += err_num
        day_err_all[ day_idx - day_st ]     += err_num
        week_err_all[ week_idx - week_st ]  += err_num
        all_err_dict[ key ] += err_num
        all_err_all += err_num

    for key  in name_dict:
        res[ "name_list" ].append( key )
        res[ "hour_err_ratio" ].append( common_method.getListDivide( hour_err_dict[ key ] , hour_num_dict[ key ] , 100.0 ) )
        res[ "hour_err_percent" ].append( common_method.getListDivide( hour_err_dict[ key ] , hour_err_all , 100.0 ) )
        res[ "day_err_ratio" ].append( common_method.getListDivide( day_err_dict[ key ] , day_num_dict[ key ] , 100.0 ) )
        res[ "day_err_percent" ].append( common_method.getListDivide( day_err_dict[ key ] , day_err_all , 100.0 ) )
        res[ "week_err_ratio" ].append( common_method.getListDivide( week_err_dict[ key ] , week_num_dict[ key ] , 100.0 ) )
        res[ "week_err_percent" ].append( common_method.getListDivide( week_err_dict[ key ] , week_err_all , 100.0 ) )
        res[ "all_err_ratio" ].append( all_err_dict[ key ] * 100.0 / all_num_dict[ key ] if all_num_dict[ key ] > 0 else 0 )
        res[ "all_err_percent" ].append( all_err_dict[ key ] * 100.0 / all_err_all if all_err_all > 0 else 0 )
    return res   
    
 
    
def query_port_illegal_query( st_time , ed_time ):
    global conn
    global cur
    global sum_dict

    res = {}
    res[ "data_len" ]           = 0
    res[ "name_list" ]          = []
    
    res[ "hour_range" ]         = []
    res[ "hour_illegal_ratio" ]     = []
    res[ "hour_illegal_percent" ]   = []
    res[ "hour_illegal_num" ]       = []
    res[ "hour_total_illegal_ratio" ] = []
    res[ "hour_total_illegal_num" ]   = []
    
    res[ "day_range" ]         = []
    res[ "day_illegal_ratio" ]     = []
    res[ "day_illegal_percent" ]   = []
    res[ "day_illegal_num" ]       = []
    res[ "day_total_illegal_ratio" ] = []
    res[ "day_total_illegal_num" ]   = []
    res[ "day_week_illegal_ratio" ]     = []
    res[ "day_week_illegal_percent" ]   = []
    res[ "day_week_total_illegal_ratio" ]   = []
    
    res[ "week_range" ]         = []
    res[ "week_illegal_ratio" ]     = []
    res[ "week_illegal_percent" ]   = []
    res[ "week_illegal_num" ]       = []
    res[ "week_total_illegal_ratio" ] = []
    res[ "week_total_illegal_num" ]   = []
    res[ "week_day_illegal_ratio" ]     = []
    res[ "week_day_illegal_percent" ]   = []
    res[ "week_day_total_illegal_ratio" ]   = []
    res[ "all_illegal_ratio" ]      = []
    res[ "all_illegal_percent" ]    = []

    cmd = "select from_info , resid , qt , version , time , query_num , illegal_num from " + sum_dict[ "qt_version_time" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[4] , y[4] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
        
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
        
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 4 ] , qres[ -1 ][ 4 ] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    day_to_week_idx , week_day_count = day_zero_list[0:] , week_zero_list[0:]
    for i in range( len( day_to_week_idx ) ):
        day_to_week_idx[i] = common_method.UTChourToWeekIdx( (i+day_st)*24 ) - week_st
        week_day_count[ day_to_week_idx[i] ] += 1
    
    name_dict , hour_num_dict , day_num_dict , week_num_dict , all_num_dict = {} , {} , {} , {} ,{}
    hour_illegal_dict , day_illegal_dict , week_illegal_dict , all_illegal_dict = {} , {} , {} , {}
    hour_illegal_all , day_illegal_all , week_illegal_all = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    hour_pv_all , day_pv_all , week_pv_all = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    all_illegal_all = 0
    
    for qr in qres :
        key = from_resid_qt_version_to_port_name( qr[0] , qr[1] , qr[2] , qr[3] )
        if key not in name_dict:
            name_dict[ key ] = 1
            hour_num_dict[ key ] , hour_illegal_dict[ key ]    = hour_zero_list[0:] , hour_zero_list[0:]
            day_num_dict[ key ] , day_illegal_dict[ key ]      = day_zero_list[0:] , day_zero_list[0:]
            week_num_dict[ key ] , week_illegal_dict[ key ]    = week_zero_list[0:] , week_zero_list[0:]
            all_num_dict[ key ] , all_illegal_dict[ key ]      = 0 , 0
        query_num , illegal_num = string.atoi( qr[5] ) + string.atoi( qr[6] ) , string.atoi( qr[6] )
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[4] ) )
        hour_num_dict[ key ][ hour_idx - hour_st ]  += query_num
        day_num_dict[ key ][ day_idx - day_st ]     += query_num
        week_num_dict[ key ][ week_idx - week_st ]  += query_num
        all_num_dict[ key ] += query_num
        hour_illegal_dict[ key ][ hour_idx - hour_st ]  += illegal_num
        day_illegal_dict[ key ][ day_idx - day_st ]     += illegal_num
        week_illegal_dict[ key ][ week_idx - week_st ]  += illegal_num
        hour_pv_all[ hour_idx - hour_st ]  += query_num
        day_pv_all[ day_idx - day_st ]     += query_num
        week_pv_all[ week_idx - week_st ]  += query_num
        hour_illegal_all[ hour_idx - hour_st ]  += illegal_num
        day_illegal_all[ day_idx - day_st ]     += illegal_num
        week_illegal_all[ week_idx - week_st ]  += illegal_num
        all_illegal_dict[ key ] += illegal_num
        all_illegal_all += illegal_num

    for key  in name_dict:
        res[ "name_list" ].append( key )
        res[ "hour_illegal_num" ].append( hour_illegal_dict[key] )
        res[ "hour_illegal_ratio" ].append( common_method.getListDivide( hour_illegal_dict[ key ] , hour_num_dict[ key ] , 100.0 ) )
        res[ "hour_illegal_percent" ].append( common_method.getListDivide( hour_illegal_dict[ key ] , hour_illegal_all , 100.0 ) )
        res[ "day_illegal_num" ].append( day_illegal_dict[key] )
        res[ "day_illegal_ratio" ].append( common_method.getListDivide( day_illegal_dict[ key ] , day_num_dict[ key ] , 100.0 ) )
        res[ "day_illegal_percent" ].append( common_method.getListDivide( day_illegal_dict[ key ] , day_illegal_all , 100.0 ) )
        res[ "week_illegal_num" ].append( week_illegal_dict[key] )
        res[ "week_illegal_ratio" ].append( common_method.getListDivide( week_illegal_dict[ key ] , week_num_dict[ key ] , 100.0 ) )
        res[ "week_illegal_percent" ].append( common_method.getListDivide( week_illegal_dict[ key ] , week_illegal_all , 100.0 ) )
        res[ "all_illegal_ratio" ].append( all_illegal_dict[ key ] * 100.0 / all_num_dict[ key ] if all_num_dict[ key ] > 0 else 0 )
        res[ "all_illegal_percent" ].append( all_illegal_dict[ key ] * 100.0 / all_illegal_all if all_illegal_all > 0 else 0 )
        res[ "day_week_illegal_ratio" ].append( common_method.getDayWeekAvg( res[ "day_illegal_ratio" ][-1] , day_to_week_idx , week_day_count ) )
        res[ "day_week_illegal_percent" ].append( common_method.getDayWeekAvg( res[ "day_illegal_percent" ][-1] , day_to_week_idx , week_day_count ) )
        res[ "week_day_illegal_ratio" ].append( common_method.getWeekDayAvg( res[ "day_illegal_ratio" ][-1] , day_to_week_idx , week_day_count ) )
        res[ "week_day_illegal_percent" ].append( common_method.getWeekDayAvg( res[ "day_illegal_percent" ][-1] , day_to_week_idx , week_day_count ) )
    res[ "hour_total_illegal_num" ] = hour_illegal_all
    res[ "day_total_illegal_num" ]  = day_illegal_all
    res[ "week_total_illegal_num" ] = week_illegal_all
    res[ "hour_total_illegal_ratio" ] = common_method.getListDivide( hour_illegal_all , hour_pv_all , 100.0 )
    res[ "day_total_illegal_ratio" ]  = common_method.getListDivide( day_illegal_all , day_pv_all , 100.0 )
    res[ "week_total_illegal_ratio" ] = common_method.getListDivide( week_illegal_all , week_pv_all , 100.0 )
    res[ "day_week_total_illegal_ratio" ] = common_method.getDayWeekAvg( res["day_total_illegal_ratio"] , day_to_week_idx , week_day_count )
    res[ "week_day_total_illegal_ratio" ] = common_method.getWeekDayAvg( res["day_total_illegal_ratio"] , day_to_week_idx , week_day_count )
    return res    


def query_port2version_illegal_query( st_time , ed_time , port_name ):
    global conn
    global cur
    global sum_dict

    res = {}
    res[ "data_len" ]           = 0
    res[ "name_list" ]          = []
    
    res[ "hour_range" ]         = []
    res[ "hour_illegal_ratio" ]     = []
    res[ "hour_illegal_percent" ]   = []
    res[ "hour_illegal_num" ]       = []
    
    res[ "day_range" ]         = []
    res[ "day_illegal_ratio" ]     = []
    res[ "day_illegal_percent" ]   = []
    res[ "day_illegal_num" ]       = []
    
    res[ "week_range" ]         = []
    res[ "week_illegal_ratio" ]     = []
    res[ "week_illegal_percent" ]   = []
    res[ "week_illegal_num" ]       = []

    res[ "all_illegal_ratio" ]      = []
    res[ "all_illegal_percent" ]    = []

    cmd = "select from_info , resid , qt , version , time , query_num , illegal_num from " + sum_dict[ "qt_version_time" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[4] , y[4] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    #print qres
        
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
        
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 4 ] , qres[ -1 ][ 4 ] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    
    name_dict , hour_num_dict , day_num_dict , week_num_dict , all_num_dict = {} , {} , {} , {} ,{}
    hour_illegal_dict , day_illegal_dict , week_illegal_dict , all_illegal_dict = {} , {} , {} , {}
    hour_illegal_all , day_illegal_all , week_illegal_all = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    all_illegal_all = 0
    
    for qr in qres :
        key = from_resid_qt_version_to_port_name( qr[0] , qr[1] , qr[2] , qr[3] )
        if key != port_name :
            continue
        key = qr[2] + ',' + qr[3]
        if key not in name_dict:
            name_dict[ key ] = 1
            hour_num_dict[ key ] , hour_illegal_dict[ key ]    = hour_zero_list[0:] , hour_zero_list[0:]
            day_num_dict[ key ] , day_illegal_dict[ key ]      = day_zero_list[0:] , day_zero_list[0:]
            week_num_dict[ key ] , week_illegal_dict[ key ]    = week_zero_list[0:] , week_zero_list[0:]
            all_num_dict[ key ] , all_illegal_dict[ key ]      = 0 , 0
        query_num , illegal_num = string.atoi( qr[5] ) + string.atoi( qr[6] ) , string.atoi( qr[6] )
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[4] ) )
        hour_num_dict[ key ][ hour_idx - hour_st ]  += query_num
        day_num_dict[ key ][ day_idx - day_st ]     += query_num
        week_num_dict[ key ][ week_idx - week_st ]  += query_num
        all_num_dict[ key ] += query_num
        hour_illegal_dict[ key ][ hour_idx - hour_st ]  += illegal_num
        day_illegal_dict[ key ][ day_idx - day_st ]     += illegal_num
        week_illegal_dict[ key ][ week_idx - week_st ]  += illegal_num
        hour_illegal_all[ hour_idx - hour_st ]  += illegal_num
        day_illegal_all[ day_idx - day_st ]     += illegal_num
        week_illegal_all[ week_idx - week_st ]  += illegal_num
        all_illegal_dict[ key ] += illegal_num
        all_illegal_all += illegal_num

    for key  in name_dict:
        res[ "name_list" ].append( key )
        res[ "hour_illegal_num" ].append( hour_illegal_dict[ key ] ) 
        res[ "hour_illegal_ratio" ].append( common_method.getListDivide( hour_illegal_dict[ key ] , hour_num_dict[ key ] , 100.0 ) )
        res[ "hour_illegal_percent" ].append( common_method.getListDivide( hour_illegal_dict[ key ] , hour_illegal_all , 100.0 ) )
        res[ "day_illegal_num" ].append( day_illegal_dict[ key ] )
        res[ "day_illegal_ratio" ].append( common_method.getListDivide( day_illegal_dict[ key ] , day_num_dict[ key ] , 100.0 ) )
        res[ "day_illegal_percent" ].append( common_method.getListDivide( day_illegal_dict[ key ] , day_illegal_all , 100.0 ) )
        res[ "week_illegal_num" ].append( week_illegal_dict[ key ] )
        res[ "week_illegal_ratio" ].append( common_method.getListDivide( week_illegal_dict[ key ] , week_num_dict[ key ] , 100.0 ) )
        res[ "week_illegal_percent" ].append( common_method.getListDivide( week_illegal_dict[ key ] , week_illegal_all , 100.0 ) )
        res[ "all_illegal_ratio" ].append( all_illegal_dict[ key ] * 100.0 / all_num_dict[ key ] if all_num_dict[ key ] > 0 else 0 )
        res[ "all_illegal_percent" ].append( all_illegal_dict[ key ] * 100.0 / all_illegal_all if all_illegal_all > 0 else 0 )
    return res   
    


 
    
def query_port_data_query( st_time , ed_time ):
    global conn
    global cur
    global sum_dict
    data_vector = qvt_size_vec[0:]
    res = {}
    res[ "data_len" ]       = 0
    res[ "hour_range" ]     = []
    res[ "day_range" ]      = []
    res[ "week_range" ]     = []
    res[ "name_list" ]      = []
    res[ "hour_data_list" ] = []
    res[ "day_data_list" ]  = []
    res[ "day_week_data_list" ]  = []
    res[ "week_data_list" ] = []
    res[ "week_day_data_list" ] = []
    res[ "hour_total_data" ] = []
    res[ "day_total_data" ]  = []
    res[ "day_week_total_data" ]  = []
    res[ "week_total_data" ] = []
    res[ "week_day_total_data" ] = []
    res[ "all_data_list" ]  = []
    res[ "data_vector" ]    = data_vector
    res[ "data_div_list" ]  = []
    
    cmd = "select from_info , resid , qt , version , time , size , size_vector , query_num from " + sum_dict[ "qt_version_time" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[4] , y[4] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    #print qres
        
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
        
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 4 ] , qres[ -1 ][ 4 ] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    size_zero_list  = [ 0 for i in range( 1 + len( data_vector ) ) ]
    day_to_week_idx , week_day_count = day_zero_list[0:] , week_zero_list[0:]
    for i in range( len( day_to_week_idx ) ):
        day_to_week_idx[i] = common_method.UTChourToWeekIdx( (i+day_st)*24 ) - week_st
        week_day_count[ day_to_week_idx[i] ] += 1
    
    name_dict , hour_all_dict , day_all_dict , week_all_dict , data_all_dict = {} , {} , {} , {} ,{}
    hour_num_dict , day_num_dict , week_num_dict , data_num_dict = {} , {} , {} , {}
    data_div_dict   = {}
    
    hour_total_pv , day_total_pv , week_total_pv = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    hour_total_ds , day_total_ds , week_total_ds = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    
    for qr in qres :
        key = from_resid_qt_version_to_port_name( qr[0] , qr[1] , qr[2] , qr[3] )
        if key not in name_dict:
            hour_all_dict[ key ]    = hour_zero_list[ 0 : ]
            hour_num_dict[ key ]    = hour_zero_list[ 0 : ]
            day_all_dict[ key ]     = day_zero_list[ 0 : ]
            day_num_dict[ key ]     = day_zero_list[ 0 : ]
            week_all_dict[ key ]    = week_zero_list[ 0 : ]
            week_num_dict[ key ]    = week_zero_list[ 0 : ]
            data_div_dict[ key ]    = size_zero_list[ 0 : ]
            data_all_dict[ key ]    = 0
            data_num_dict[ key ]    = 0
            name_dict[ key ]        = 1
        total_size , query_num = string.atoi( qr[5] ) , string.atoi( qr[7] )
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[4] ) )
        hour_all_dict[ key ][ hour_idx - hour_st ]  += total_size
        hour_num_dict[ key ][ hour_idx - hour_st ]  += query_num
        day_all_dict[ key ][ day_idx - day_st ]     += total_size
        day_num_dict[ key ][ day_idx - day_st ]     += query_num
        week_all_dict[ key ][ week_idx - week_st ]  += total_size
        week_num_dict[ key ][ week_idx - week_st ]  += query_num
        data_all_dict[ key ]    += total_size
        data_num_dict[ key ]    += query_num
        hour_total_ds[ hour_idx - hour_st ]  += total_size
        day_total_ds[ day_idx - day_st ]     += total_size
        week_total_ds[ week_idx - week_st ]  += total_size
        hour_total_pv[ hour_idx - hour_st ]  += query_num
        day_total_pv[ day_idx - day_st ]     += query_num
        week_total_pv[ week_idx - week_st ]  += query_num
        one_div_list        = size_zero_list[ 0 : ]
        if len( qr[6] ) >= len( size_zero_list ) :
            one_div_list    = [ string.atoi( qrr ) for qrr in qr[6].split( '|' ) ] 
        data_div_dict[ key ]    = common_method.getListAdd( one_div_list , data_div_dict[ key ] )
        
    for  k in name_dict :
        query_num = common_method.getListSum( data_div_dict[ k ] )
        res[ "name_list" ].append( k )
        res[ "hour_data_list" ].append( common_method.getListDivide( hour_all_dict[ k ] , hour_num_dict[ k ] , 1.0 ) )
        res[ "day_data_list" ].append( common_method.getListDivide( day_all_dict[ k ] ,day_num_dict[ k ] , 1.0 ) )
        res[ "week_data_list" ].append( common_method.getListDivide( week_all_dict[ k ] ,week_num_dict[ k ] , 1.0 ) )
        res[ "all_data_list" ].append( data_all_dict[ k ] / data_num_dict[ k ] if data_num_dict[ k ] > 0 else 0 )
        res[ "data_div_list" ].append( [ ddd * 100.0 / query_num  if query_num > 0 else  0 for ddd in data_div_dict[ k ] ] )
        res[ "day_week_data_list" ].append( common_method.getDayWeekAvg( res["day_data_list"][-1] , day_to_week_idx , week_day_count ) )
        res[ "week_day_data_list" ].append( common_method.getWeekDayAvg( res["day_data_list"][-1] , day_to_week_idx , week_day_count ) )
    res[ "hour_total_data" ] = common_method.getListDivide( hour_total_ds , hour_total_pv , 1.0 )
    res[ "day_total_data" ]  = common_method.getListDivide( day_total_ds , day_total_pv , 1.0 )
    res[ "week_total_data" ] = common_method.getListDivide( week_total_ds , week_total_pv , 1.0 )
    res[ "day_week_total_data" ] = common_method.getDayWeekAvg( res[ "day_total_data" ] , day_to_week_idx , week_day_count ) 
    res[ "week_day_total_data" ] = common_method.getWeekDayAvg( res[ "day_total_data" ] , day_to_week_idx , week_day_count ) 
    return res    


def query_port2version_data_query( st_time , ed_time , port_name ):
    global conn
    global cur
    global sum_dict
    data_vector = qvt_size_vec[0:]
    res = {}
    res[ "data_len" ]       = 0
    res[ "hour_range" ]     = [];
    res[ "day_range" ]      = [];
    res[ "week_range" ]     = [];
    res[ "name_list" ]      = []
    res[ "hour_data_list" ] = []
    res[ "day_data_list" ]  = []
    res[ "week_data_list" ] = []
    res[ "all_data_list" ]  = []
    res[ "data_vector" ]    = data_vector
    res[ "data_div_list" ]  = []
    
    cmd = "select from_info , resid , qt , version , time , size , size_vector , query_num from " + sum_dict[ "qt_version_time" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[4] , y[4] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    #print qres
        
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
        
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 4 ] , qres[ -1 ][ 4 ] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    size_zero_list  = [ 0 for i in range( 1 + len( data_vector ) ) ]
    
    name_dict , hour_all_dict , day_all_dict , week_all_dict , data_all_dict = {} , {} , {} , {} ,{}
    hour_num_dict , day_num_dict , week_num_dict , data_num_dict = {} , {} , {} , {}
    data_div_dict   = {}
    
    for qr in qres :
        key = from_resid_qt_version_to_port_name( qr[0] , qr[1] , qr[2] , qr[3] )
        if key != port_name :
            continue
        key = qr[2] + ',' + qr[3]
        if key not in name_dict:
            hour_all_dict[ key ]    = hour_zero_list[ 0 : ]
            hour_num_dict[ key ]    = hour_zero_list[ 0 : ]
            day_all_dict[ key ]     = day_zero_list[ 0 : ]
            day_num_dict[ key ]     = day_zero_list[ 0 : ]
            week_all_dict[ key ]    = week_zero_list[ 0 : ]
            week_num_dict[ key ]    = week_zero_list[ 0 : ]
            data_div_dict[ key ]    = size_zero_list[ 0 : ]
            data_all_dict[ key ]    = 0
            data_num_dict[ key ]    = 0
            name_dict[ key ]        = 1
        total_size , query_num = string.atoi( qr[5] ) , string.atoi( qr[7] )
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[4] ) )
        hour_all_dict[ key ][ hour_idx - hour_st ]  += total_size
        hour_num_dict[ key ][ hour_idx - hour_st ]  += query_num
        day_all_dict[ key ][ day_idx - day_st ]     += total_size
        day_num_dict[ key ][ day_idx - day_st ]     += query_num
        week_all_dict[ key ][ week_idx - week_st ]  += total_size
        week_num_dict[ key ][ week_idx - week_st ]  += query_num
        data_all_dict[ key ]    += total_size
        data_num_dict[ key ]    += query_num
        one_div_list        = size_zero_list[ 0 : ]
        if len( qr[6] ) >= len( size_zero_list ) :
            one_div_list    = [ string.atoi( qrr ) for qrr in qr[6].split( '|' ) ] 
        data_div_dict[ key ]    = common_method.getListAdd( one_div_list , data_div_dict[ key ] )
        
    for  k in name_dict :
        query_num = common_method.getListSum( data_div_dict[ k ] )
        res[ "name_list" ].append( k )
        res[ "hour_data_list" ].append( common_method.getListDivide( hour_all_dict[ k ] , hour_num_dict[ k ] , 1.0 ) )
        res[ "day_data_list" ].append( common_method.getListDivide( day_all_dict[ k ] ,day_num_dict[ k ] , 1.0 ) )
        res[ "week_data_list" ].append( common_method.getListDivide( week_all_dict[ k ] ,week_num_dict[ k ] , 1.0 ) )
        res[ "all_data_list" ].append( data_all_dict[ k ] / data_num_dict[ k ] if data_num_dict[ k ] > 0 else 0 )
        res[ "data_div_list" ].append( [ ddd * 100.0 / query_num  if query_num > 0 else  0 for ddd in data_div_dict[ k ] ] )
    return res    


    
   
    
def query_port_performance_query( st_time , ed_time ):
    global conn
    global cur
    global sum_dict
    cmd = "select from_info , resid , qt , version , time , query_num , avg_time , time_vector , overtime_num from " + sum_dict[ "qt_version_time" ] ;
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    time_vector = qvt_time_vec[0:]
    
    res = {}
    res[ "data_len" ]       = 0
    res[ "name_list" ]      = []
    res[ "hour_range" ]     = []
    res[ "day_range" ]      = []
    res[ "week_range" ]     = []
    res[ "hour_data_list" ] = []
    res[ "day_data_list" ]  = []
    res[ "week_data_list" ] = []
    res[ "hour_total_avg_time" ] = []
    res[ "day_total_avg_time" ]  = []
    res[ "week_total_avg_time" ] = []
    res[ "all_data_list" ]  = []
    res[ "data_vector" ]    = time_vector
    res[ "data_div_list" ]  = []
    res[ "data_div_num_list" ]  = []
    
    res[ "hour_ot_num_list" ] = []
    res[ "day_ot_num_list" ]  = []
    res[ "week_ot_num_list" ] = []
    res[ "hour_ot_ratio_list" ] = []
    res[ "day_ot_ratio_list" ]  = []
    res[ "week_ot_ratio_list" ] = []
    res[ "hour_total_ot_num_list" ] = []
    res[ "day_total_ot_num_list" ]  = []
    res[ "week_total_ot_num_list" ] = []
    res[ "hour_total_ot_ratio_list" ] = []
    res[ "day_total_ot_ratio_list" ]  = []
    res[ "week_total_ot_ratio_list" ] = []
    
    res[ "day_week_data_list" ]  = []
    res[ "week_day_data_list" ]  = []
    res[ "day_week_total_avg_time" ] = []
    res[ "week_day_total_avg_time" ] = []
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[4] , y[4] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    #print qres
        
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
        
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 4 ] , qres[ -1 ][ 4 ] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    time_zero_list  = [ 0 for i in range( 1 + len( time_vector ) ) ]
    day_to_week_idx , week_day_count = day_zero_list[0:] , week_zero_list[0:]
    for i in range( len( day_to_week_idx ) ):
        day_to_week_idx[i] = common_method.UTChourToWeekIdx( (i+day_st)*24 ) - week_st
        week_day_count[ day_to_week_idx[i] ] += 1
    
    name_dict       = {}
    hour_all_time_d = {}
    hour_all_num_d  = {}
    hour_ot_num_d   = {}
    day_all_time_d  = {}
    day_all_num_d   = {}
    day_ot_num_d    = {}
    week_all_time_d = {}
    week_all_num_d  = {}
    week_ot_num_d   = {}
    data_all_time_d = {}
    data_all_num_d  = {}
    data_div_dict   = {}
    hour_total_time , day_total_time , week_total_time = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    hour_total_pv , day_total_pv , week_total_pv = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    hour_total_ot_num_list, day_total_ot_num_list, week_total_ot_num_list = hour_zero_list[0:], day_zero_list[0:], week_zero_list[0:]
    
    for qr in qres :
        key = from_resid_qt_version_to_port_name( qr[0] , qr[1] , qr[2] , qr[3] )
        if key not in name_dict:
            name_dict[ key ]        = 1
            hour_all_time_d[ key ]  = hour_zero_list[ 0 : ]
            hour_all_num_d[ key ]   = hour_zero_list[ 0 : ]
            hour_ot_num_d[ key ]    = hour_zero_list[ 0 : ]
            day_all_time_d[ key ]   = day_zero_list[ 0 : ]
            day_all_num_d[ key ]    = day_zero_list[ 0 : ]
            day_ot_num_d[ key ]     = day_zero_list[ 0 : ]
            week_all_time_d[ key ]  = week_zero_list[ 0 : ]
            week_all_num_d[ key ]   = week_zero_list[ 0 : ]
            week_ot_num_d[ key ]    = week_zero_list[ 0 : ]
            data_div_dict[ key ]    = time_zero_list[ 0 : ]
            data_all_time_d[ key ]  = 0
            data_all_num_d[ key ]   = 0
        query_num , avg_time , ot_num = string.atoi( qr[5] ) , string.atof( qr[6] ) , string.atoi( qr[8] )
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[4] ) )
        hour_all_time_d[ key ][ hour_idx - hour_st ]+= avg_time * query_num
        hour_all_num_d[ key ][ hour_idx - hour_st ] += query_num
        hour_ot_num_d[ key ][ hour_idx - hour_st ]  += ot_num
        day_all_time_d[ key ][ day_idx - day_st ]   += avg_time * query_num
        day_all_num_d[ key ][ day_idx - day_st ]    += query_num
        day_ot_num_d[ key ][ day_idx - day_st ]     += ot_num
        week_all_time_d[ key ][ week_idx - week_st ]+= avg_time * query_num
        week_all_num_d[ key ][ week_idx - week_st ] += query_num
        week_ot_num_d[ key ][ week_idx - week_st ]  += ot_num
        data_all_time_d[ key ]  += avg_time * query_num
        data_all_num_d[ key ]   += query_num
        hour_total_pv[ hour_idx - hour_st ] += query_num
        day_total_pv[ day_idx - day_st ]    += query_num
        week_total_pv[ week_idx - week_st ] += query_num
        hour_total_time[ hour_idx - hour_st ] += avg_time * query_num
        day_total_time[ day_idx - day_st ] += avg_time * query_num
        week_total_time[ week_idx - week_st ] += avg_time * query_num
        hour_total_ot_num_list[ hour_idx - hour_st ] += ot_num
        day_total_ot_num_list[ day_idx - day_st ]    += ot_num
        week_total_ot_num_list[ week_idx - week_st ] += ot_num
        one_div_list        = time_zero_list[ 0 : ]
        if qr[ 7 ] != "" :
            one_div_list    = [ string.atoi( qrr ) for qrr in qr[ 7 ].split( '|' ) ] 
            data_div_dict[ key ]    = common_method.getListAdd( one_div_list , data_div_dict[ key ] )
        
    for  k in name_dict :
        query_num = common_method.getListSum( data_div_dict[ k ] )
        res[ "name_list" ].append( k )
        res[ "hour_data_list" ].append( common_method.getListDivide( hour_all_time_d[ k ] , hour_all_num_d[k] , 1.0 ) )
        res[ "day_data_list" ].append( common_method.getListDivide( day_all_time_d[ k ] , day_all_num_d[k] , 1.0 ) )
        res[ "week_data_list" ].append( common_method.getListDivide( week_all_time_d[ k ] , week_all_num_d[k] , 1.0 ) )
        res[ "all_data_list" ].append( data_all_time_d[ k ] / data_all_num_d[ k ] if data_all_num_d[ k ] > 0 else 0 )
        res[ "data_div_list" ].append( [ ddd * 100.0 / query_num  if query_num > 0 else  0  for ddd in data_div_dict[ k ] ] )
        res[ "data_div_num_list" ].append( data_div_dict[ k ] )
        res[ "hour_ot_num_list" ].append( hour_ot_num_d[ k ] )
        res[ "hour_ot_ratio_list" ].append( common_method.getListDivide( hour_ot_num_d[ k ] , hour_all_num_d[k] , 100.0 ) )
        res[ "day_ot_num_list" ].append( day_ot_num_d[ k ] )
        res[ "day_ot_ratio_list" ].append( common_method.getListDivide( day_ot_num_d[ k ] , day_all_num_d[k] , 100.0 ) )
        res[ "week_ot_num_list" ].append( week_ot_num_d[ k ] )
        res[ "week_ot_ratio_list" ].append( common_method.getListDivide( week_ot_num_d[ k ] , week_all_num_d[k] , 100.0 ) )
        res[ "day_week_data_list" ].append( common_method.getDayWeekAvg( res["day_data_list"][-1] , day_to_week_idx , week_day_count ) )
        res[ "week_day_data_list" ].append( common_method.getWeekDayAvg( res["day_data_list"][-1] , day_to_week_idx , week_day_count ) )
    res[ "hour_total_avg_time" ] = common_method.getListDivide( hour_total_time , hour_total_pv , 1.0 )
    res[ "day_total_avg_time" ]  = common_method.getListDivide( day_total_time , day_total_pv , 1.0 )
    res[ "week_total_avg_time" ] = common_method.getListDivide( week_total_time , week_total_pv , 1.0 )
    res[ "day_week_total_avg_time" ] = common_method.getDayWeekAvg( res["day_total_avg_time"] , day_to_week_idx , week_day_count )
    res[ "week_day_total_avg_time" ] = common_method.getWeekDayAvg( res["day_total_avg_time"] , day_to_week_idx , week_day_count )
    res[ "hour_total_ot_num_list" ] = hour_total_ot_num_list
    res[ "day_total_ot_num_list" ]  = day_total_ot_num_list
    res[ "week_total_ot_num_list" ] = week_total_ot_num_list
    res[ "hour_total_ot_ratio_list" ] = common_method.getListDivide( hour_total_ot_num_list , hour_total_pv , 100.0 )
    res[ "day_total_ot_ratio_list" ]  = common_method.getListDivide( day_total_ot_num_list , day_total_pv , 100.0 )
    res[ "week_total_ot_ratio_list" ] = common_method.getListDivide( week_total_ot_num_list , week_total_pv , 100.0 )
    return res
 


def query_port2version_performance_query( st_time , ed_time , port_name ):
    global conn
    global cur
    global sum_dict
    cmd = "select from_info , resid , qt , version , time , query_num , avg_time , time_vector , overtime_num from " + sum_dict[ "qt_version_time" ] ;
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    time_vector = qvt_time_vec[0:]
    
    res = {}
    res[ "data_len" ]       = 0
    res[ "name_list" ]      = []
    res[ "hour_range" ]     = []
    res[ "day_range" ]      = []
    res[ "week_range" ]     = []
    res[ "hour_data_list" ] = []
    res[ "day_data_list" ]  = []
    res[ "week_data_list" ] = []
    res[ "all_data_list" ]  = []
    res[ "data_vector" ]    = time_vector
    res[ "data_div_list" ]  = []
    res[ "data_div_num_list" ] = []
    
    res[ "hour_ot_num_list" ] = []
    res[ "day_ot_num_list" ]  = []
    res[ "week_ot_num_list" ] = []
    res[ "hour_ot_ratio_list" ] = []
    res[ "day_ot_ratio_list" ]  = []
    res[ "week_ot_ratio_list" ] = []
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[4] , y[4] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    #print qres
        
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
        
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 4 ] , qres[ -1 ][ 4 ] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    time_zero_list  = [ 0 for i in range( 1 + len( time_vector ) ) ]
    
    name_dict       = {}
    hour_all_time_d = {}
    hour_all_num_d  = {}
    hour_ot_num_d   = {}
    day_all_time_d  = {}
    day_all_num_d   = {}
    day_ot_num_d    = {}
    week_all_time_d = {}
    week_all_num_d  = {}
    week_ot_num_d   = {}
    data_all_time_d = {}
    data_all_num_d  = {}
    data_div_dict   = {}
    
    for qr in qres :
        key = from_resid_qt_version_to_port_name( qr[0] , qr[1] , qr[2] , qr[3] )
        if key != port_name:
            continue
        key = qr[2] + ',' + qr[3]
        if key not in name_dict:
            name_dict[ key ]        = 1
            hour_all_time_d[ key ]  = hour_zero_list[ 0 : ]
            hour_all_num_d[ key ]   = hour_zero_list[ 0 : ]
            hour_ot_num_d[ key ]    = hour_zero_list[ 0 : ]
            day_all_time_d[ key ]   = day_zero_list[ 0 : ]
            day_all_num_d[ key ]    = day_zero_list[ 0 : ]
            day_ot_num_d[ key ]     = day_zero_list[ 0 : ]
            week_all_time_d[ key ]  = week_zero_list[ 0 : ]
            week_all_num_d[ key ]   = week_zero_list[ 0 : ]
            week_ot_num_d[ key ]    = week_zero_list[ 0 : ]
            data_div_dict[ key ]    = time_zero_list[ 0 : ]
            data_all_time_d[ key ]  = 0
            data_all_num_d[ key ]   = 0
        query_num , avg_time , ot_num = string.atoi( qr[5] ) , string.atof( qr[6] ) , string.atoi( qr[8] )
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[4] ) )
        hour_all_time_d[ key ][ hour_idx - hour_st ]+= avg_time * query_num
        hour_all_num_d[ key ][ hour_idx - hour_st ] += query_num
        hour_ot_num_d[ key ][ hour_idx - hour_st ]  += ot_num
        day_all_time_d[ key ][ day_idx - day_st ]   += avg_time * query_num
        day_all_num_d[ key ][ day_idx - day_st ]    += query_num
        day_ot_num_d[ key ][ day_idx - day_st ]     += ot_num
        week_all_time_d[ key ][ week_idx - week_st ]+= avg_time * query_num
        week_all_num_d[ key ][ week_idx - week_st ] += query_num
        week_ot_num_d[ key ][ week_idx - week_st ]  += ot_num
        data_all_time_d[ key ]  += avg_time * query_num
        data_all_num_d[ key ]   += query_num
        one_div_list        = time_zero_list[ 0 : ]
        if  qr[ 7 ]!="" :
            one_div_list    = [ string.atoi( qrr ) for qrr in qr[ 7 ].split( '|' ) ] 
            data_div_dict[ key ]    = common_method.getListAdd( one_div_list , data_div_dict[ key ] )
        
    for  k in name_dict :
        query_num = common_method.getListSum( data_div_dict[ k ] )
        res[ "name_list" ].append( k )
        res[ "hour_data_list" ].append( common_method.getListDivide( hour_all_time_d[ k ] , hour_all_num_d[k] , 1.0 ) )
        res[ "day_data_list" ].append( common_method.getListDivide( day_all_time_d[ k ] , day_all_num_d[k] , 1.0 ) )
        res[ "week_data_list" ].append( common_method.getListDivide( week_all_time_d[ k ] , week_all_num_d[k] , 1.0 ) )
        res[ "all_data_list" ].append( data_all_time_d[ k ] / data_all_num_d[ k ] if data_all_num_d[ k ] > 0 else 0 )
        res[ "data_div_list" ].append( [ ddd * 100.0 / query_num  if query_num > 0 else  0  for ddd in data_div_dict[ k ] ] )   
        res[ "data_div_num_list" ].append( data_div_dict[ k ] )   
        res[ "hour_ot_num_list" ].append( hour_ot_num_d[ k ] )
        res[ "hour_ot_ratio_list" ].append( common_method.getListDivide( hour_ot_num_d[ k ] , hour_all_num_d[k] , 100.0 ) )
        res[ "day_ot_num_list" ].append( day_ot_num_d[ k ] )
        res[ "day_ot_ratio_list" ].append( common_method.getListDivide( day_ot_num_d[ k ] , day_all_num_d[k] , 100.0 ) )
        res[ "week_ot_num_list" ].append( week_ot_num_d[ k ] )
        res[ "week_ot_ratio_list" ].append( common_method.getListDivide( week_ot_num_d[ k ] , week_all_num_d[k] , 100.0 ) )
    return res
 
 
#select resid , time , num , yaw_num from resid_time_num_yaw where time <=b and time >=a    
def query_yawpos_summary( st_time , ed_time ) :
    res = {}
    res[ "data_len" ]   = 0
    res[ "name_list" ]  = []
    res[ "hour_range" ] = []
    res[ "day_range" ]  = []
    res[ "week_range" ] = []
    res[ "hour_yaw_num" ]     = []
    res[ "hour_yaw_ratio" ]   = []
    res[ "hour_yaw_percent"]  = []
    res[ "day_yaw_num" ]      = []
    res[ "day_yaw_ratio" ]    = []
    res[ "day_yaw_percent"]   = []
    res[ "week_yaw_num" ]     = []
    res[ "week_yaw_ratio" ]   = []
    res[ "week_yaw_percent"]  = []
    res[ "all_yaw_ratio" ]    = []
    res[ "all_yaw_percent" ]  = []
    
    res[ "hour_total_yaw_num" ]     = []
    res[ "hour_total_yaw_ratio" ]   = []
    res[ "day_total_yaw_num" ]      = []
    res[ "day_total_yaw_ratio" ]    = []
    res[ "week_total_yaw_num" ]     = []
    res[ "week_total_yaw_ratio" ]   = []
    
    res[ "day_week_yaw_num" ]         = []
    res[ "day_week_yaw_ratio" ]       = []
    res[ "day_week_yaw_percent"]      = []
    res[ "day_week_total_yaw_num" ]   = []
    res[ "day_week_total_yaw_ratio" ] = []
    res[ "week_day_yaw_num" ]         = []
    res[ "week_day_yaw_ratio" ]       = []
    res[ "week_day_yaw_percent"]      = []
    res[ "week_day_total_yaw_num" ]   = []
    res[ "week_day_total_yaw_ratio" ] = []
    
    cmd = "select resid , qt , version , time , num , yaw_num , from_info from " + sum_dict[ "resid_time_num_yaw" ] 
    cmd = cmd + " where (qt=\"multinavi\" or qt=\"navi\") and time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[3] , y[3] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
        
    if len( qres ) <=0 :
        return res
            
    res[ "data_len" ] = len( qres )
        
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 3 ] , qres[ -1 ][ 3 ] )
    
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    day_to_week_idx , week_day_count = day_zero_list[0:] , week_zero_list[0:]
    for i in range( len( day_to_week_idx ) ):
        day_to_week_idx[i] = common_method.UTChourToWeekIdx( (i+day_st)*24 ) - week_st
        week_day_count[ day_to_week_idx[i] ] += 1
    
    hour_yaw_num , hour_query_num , day_yaw_num , day_query_num , week_yaw_num , week_query_num  = {} , {} , {} , {} , {} , {}
    hour_yaw_all_num , day_yaw_all_num , week_yaw_all_num = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    hour_total_pv , day_total_pv , week_total_pv = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    yaw_all_num_qt , query_all_num_qt , yaw_all_num_one_port = {} , {} , 0 
    name_dict = {}
    for qr in qres:
        key = from_resid_qt_version_to_port_name( qr[6] , qr[0] , qr[1] , qr[2] )
        if key not in name_dict:
            name_dict[ key ] = 1
            hour_yaw_num[ key ] , hour_query_num[ key ] = hour_zero_list[ 0 : ] , hour_zero_list[ 0 : ]
            day_yaw_num[ key ] , day_query_num[ key ]   = day_zero_list[ 0 : ] , day_zero_list[ 0 : ]
            week_yaw_num[ key ] , week_query_num[ key ] = week_zero_list[ 0 : ] , week_zero_list[ 0 : ]
            yaw_all_num_qt[ key ] , query_all_num_qt[ key ] = 0 , 0 
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[3] ) )
        yaw_num , query_num = string.atoi( qr[5] ) , string.atoi( qr[4] )
        hour_yaw_num[ key ][ hour_idx - hour_st ]   += yaw_num
        hour_query_num[ key ][ hour_idx - hour_st ] += query_num
        day_yaw_num[ key ][ day_idx - day_st ]      += yaw_num
        day_query_num[ key ][ day_idx - day_st ]    += query_num
        week_yaw_num[ key ][ week_idx - week_st ]   += yaw_num
        week_query_num[ key ][ week_idx - week_st ] += query_num
        hour_yaw_all_num[ hour_idx - hour_st ]      += yaw_num
        day_yaw_all_num[ day_idx - day_st ]         += yaw_num
        week_yaw_all_num[ week_idx - week_st ]      += yaw_num
        hour_total_pv[ hour_idx - hour_st ]      += query_num
        day_total_pv[ day_idx - day_st ]         += query_num
        week_total_pv[ week_idx - week_st ]      += query_num
        yaw_all_num_qt[ key ]   += yaw_num
        query_all_num_qt[ key ] += query_num
        yaw_all_num_one_port    += yaw_num

    for key in name_dict:
        res[ "name_list" ].append( key )
        res[ "hour_yaw_num" ].append( hour_yaw_num[ key ] )
        res[ "hour_yaw_ratio" ].append( common_method.getListDivide( hour_yaw_num[ key ] , hour_query_num[ key ] , 100.0 ) )
        res[ "hour_yaw_percent" ].append( common_method.getListDivide( hour_yaw_num[ key ] , hour_yaw_all_num , 100.0 ) )
        res[ "day_yaw_num" ].append( day_yaw_num[ key ] )
        res[ "day_yaw_ratio" ].append( common_method.getListDivide( day_yaw_num[ key ] , day_query_num[ key ] , 100.0 ) )
        res[ "day_yaw_percent" ].append( common_method.getListDivide( day_yaw_num[ key ] , day_yaw_all_num , 100.0 ) )
        res[ "week_yaw_num" ].append( week_yaw_num[ key ] )
        res[ "week_yaw_ratio" ].append( common_method.getListDivide( week_yaw_num[ key ] , week_query_num[ key ] , 100.0 ) )
        res[ "week_yaw_percent" ].append( common_method.getListDivide( week_yaw_num[ key ] , week_yaw_all_num , 100.0 ) )
        res[ "all_yaw_ratio" ].append( yaw_all_num_qt[ key ] * 100.0 / query_all_num_qt[ key ] if query_all_num_qt[ key ] > 0 else 0 )
        res[ "all_yaw_percent" ].append( yaw_all_num_qt[ key ] * 100.0 / yaw_all_num_one_port if yaw_all_num_one_port > 0 else 0 )
        res[ "day_week_yaw_num" ].append( common_method.getDayWeekAvg(res[ "day_yaw_num" ][-1], day_to_week_idx, week_day_count) )
        res[ "day_week_yaw_ratio" ].append( common_method.getDayWeekAvg(res[ "day_yaw_ratio" ][-1], day_to_week_idx, week_day_count) )
        res[ "day_week_yaw_percent"].append( common_method.getDayWeekAvg(res[ "day_yaw_percent" ][-1], day_to_week_idx, week_day_count) )
        res[ "week_day_yaw_num" ].append( common_method.getWeekDayAvg(res[ "day_yaw_num" ][-1], day_to_week_idx, week_day_count) )
        res[ "week_day_yaw_ratio" ].append( common_method.getWeekDayAvg(res[ "day_yaw_ratio" ][-1], day_to_week_idx, week_day_count) )
        res[ "week_day_yaw_percent"].append( common_method.getWeekDayAvg(res[ "day_yaw_percent" ][-1], day_to_week_idx, week_day_count) )
    res[ "hour_total_yaw_num" ]     = hour_yaw_all_num
    res[ "hour_total_yaw_ratio" ]   = common_method.getListDivide( hour_yaw_all_num , hour_total_pv , 100.0 )
    res[ "day_total_yaw_num" ]      = day_yaw_all_num
    res[ "day_total_yaw_ratio" ]    = common_method.getListDivide( day_yaw_all_num , day_total_pv , 100.0 )
    res[ "week_total_yaw_num" ]     = week_yaw_all_num
    res[ "week_total_yaw_ratio" ]   = common_method.getListDivide( week_yaw_all_num , week_total_pv , 100.0 )
    res[ "day_week_total_yaw_num" ]   = common_method.getDayWeekAvg(res[ "day_total_yaw_num" ], day_to_week_idx, week_day_count)
    res[ "day_week_total_yaw_ratio" ] = common_method.getDayWeekAvg(res[ "day_total_yaw_ratio" ], day_to_week_idx, week_day_count)
    res[ "week_day_total_yaw_num" ]   = common_method.getWeekDayAvg(res[ "day_total_yaw_num" ], day_to_week_idx, week_day_count)
    res[ "week_day_total_yaw_ratio" ] = common_method.getWeekDayAvg(res[ "day_total_yaw_ratio" ], day_to_week_idx, week_day_count)
    return res    
    
    
def query_yawpos_type_info_summary( st_time , ed_time ) :
    res = {}
    res[ "data_len" ]   = 0
    res[ "name_list" ]  = []
    res[ "hour_range" ] = []
    res[ "day_range" ]  = []
    res[ "week_range" ] = []
    res[ "all_count_list" ]  = [] 
    res[ "hour_count_list" ] = []   
    res[ "day_count_list" ]  = []   
    res[ "week_count_list" ] = []   
    res[ "hour_yaw_percent" ] = []   
    res[ "day_yaw_percent" ]  = []   
    res[ "week_yaw_percent" ] = []  
    res[ "hour_yaw_ratio" ] = []   
    res[ "day_yaw_ratio" ]  = []   
    res[ "week_yaw_ratio" ] = []
    
    cmd = "select port_name , time , start_n , center_n , end_n , num from " + sum_dict[ "yaw_time_info_extern" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[1] , y[1] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
        
    if len( qres ) <=0 :
        return res
            
    res[ "data_len" ]   = len( qres )    
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 1 ] , qres[ -1 ][ 1 ] )
    
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    
    
    name_list = [ "start_yaw" , "center_yaw" , "end_yaw" ]
    hour_yaw_dict , day_yaw_dict , week_yaw_dict , all_yaw_dict = {} , {} , {} , {} 
    hour_count , day_count , week_count = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    hour_all_yaw , day_all_yaw , week_all_yaw = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    for key in name_list:
        hour_yaw_dict[ key ]   = hour_zero_list[0:]
        day_yaw_dict[ key ]    = day_zero_list[0:]
        week_yaw_dict[ key ]   = week_zero_list[0:]
        all_yaw_dict[ key ]    = 0
    
    for qr in qres:
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[1] ) )
        hour_yaw_dict[ "start_yaw" ][ hour_idx - hour_st ]    += string.atoi( qr[2] )
        day_yaw_dict[ "start_yaw" ][ day_idx - day_st ]       += string.atoi( qr[2] )
        week_yaw_dict[ "start_yaw" ][ week_idx - week_st ]    += string.atoi( qr[2] )
        hour_yaw_dict[ "center_yaw" ][ hour_idx - hour_st ]    += string.atoi( qr[3] )
        day_yaw_dict[ "center_yaw" ][ day_idx - day_st ]       += string.atoi( qr[3] )
        week_yaw_dict[ "center_yaw" ][ week_idx - week_st ]    += string.atoi( qr[3] )
        hour_yaw_dict[ "end_yaw" ][ hour_idx - hour_st ]    += string.atoi( qr[4] )
        day_yaw_dict[ "end_yaw" ][ day_idx - day_st ]       += string.atoi( qr[4] )
        week_yaw_dict[ "end_yaw" ][ week_idx - week_st ]    += string.atoi( qr[4] )
        all_yaw_dict[ "start_yaw" ]   += string.atoi( qr[2] )
        all_yaw_dict[ "center_yaw" ]  += string.atoi( qr[3] )
        all_yaw_dict[ "end_yaw" ]     += string.atoi( qr[4] )
        
        hour_all_yaw[ hour_idx - hour_st ]  += string.atoi( qr[2] ) + string.atoi( qr[3] ) + string.atoi( qr[4] )
        day_all_yaw[ day_idx - day_st ]     += string.atoi( qr[2] ) + string.atoi( qr[3] ) + string.atoi( qr[4] )
        week_all_yaw[ week_idx - week_st ]  += string.atoi( qr[2] ) + string.atoi( qr[3] ) + string.atoi( qr[4] )
        hour_count[ hour_idx - hour_st ]    += string.atoi( qr[5] )
        day_count[ day_idx - day_st ]       += string.atoi( qr[5] )
        week_count[ week_idx - week_st ]    += string.atoi( qr[5] )
         
    for key in name_list:
        res[ "name_list" ].append( key )
        res[ "all_count_list" ].append( all_yaw_dict[ key ] )
        res[ "hour_count_list" ].append( hour_yaw_dict[ key ] )
        res[ "day_count_list" ].append( day_yaw_dict[ key ] )
        res[ "week_count_list" ].append( week_yaw_dict[ key ] )
        res[ "hour_yaw_percent" ].append( common_method.getListDivide( hour_yaw_dict[ key ] , hour_all_yaw , 100.0 ) )
        res[ "day_yaw_percent" ].append( common_method.getListDivide( day_yaw_dict[ key ] , day_all_yaw , 100.0 ) )
        res[ "week_yaw_percent" ].append( common_method.getListDivide( week_yaw_dict[ key ] , week_all_yaw , 100.0 ) )
        res[ "hour_yaw_ratio" ].append( common_method.getListDivide( hour_yaw_dict[ key ] , hour_count , 100.0 ) )
        res[ "day_yaw_ratio" ].append( common_method.getListDivide( day_yaw_dict[ key ] , day_count , 100.0 ) )
        res[ "week_yaw_ratio" ].append( common_method.getListDivide( week_yaw_dict[ key ] , week_count , 100.0 ) )
        
    return res
  

def query_port2version_yawpos_summary( st_time , ed_time , port_name ) :
    res = {}
    res[ "data_len" ]   = 0
    res[ "name_list" ]  = []
    res[ "hour_range" ] = []
    res[ "day_range" ]  = []
    res[ "week_range" ] = []
    res[ "hour_yaw_num" ]     = []
    res[ "hour_yaw_ratio" ]   = []
    res[ "hour_yaw_percent"]  = []
    res[ "day_yaw_num" ]      = []
    res[ "day_yaw_ratio" ]    = []
    res[ "day_yaw_percent"]   = []
    res[ "week_yaw_num" ]     = []
    res[ "week_yaw_ratio" ]   = []
    res[ "week_yaw_percent"]  = []
    res[ "all_yaw_ratio" ]    = []
    res[ "all_yaw_percent" ]  = []
    
    cmd = "select resid , qt , version , time , num , yaw_num , from_info from " + sum_dict[ "resid_time_num_yaw" ] 
    cmd = cmd + " where (qt=\"multinavi\" or qt=\"navi\") and time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[3] , y[3] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
        
    if len( qres ) <=0 :
        return res
            
    res[ "data_len" ] = len( qres )
        
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 3 ] , qres[ -1 ][ 3 ] )
    
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    
    hour_yaw_num , hour_query_num , day_yaw_num , day_query_num , week_yaw_num , week_query_num  = {} , {} , {} , {} , {} , {}
    hour_yaw_all_num , day_yaw_all_num , week_yaw_all_num = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    yaw_all_num_qt , query_all_num_qt , yaw_all_num_one_port = {} , {} , 0 
    name_dict = {}
    for qr in qres:
        key = from_resid_qt_version_to_port_name( qr[6] , qr[0] , qr[1] , qr[2] )
        if key != port_name:
            continue
        key = qr[1] + "," + qr[2]
        if key not in name_dict:
            name_dict[ key ] = 1
            hour_yaw_num[ key ] , hour_query_num[ key ] = hour_zero_list[ 0 : ] , hour_zero_list[ 0 : ]
            day_yaw_num[ key ] , day_query_num[ key ]   = day_zero_list[ 0 : ] , day_zero_list[ 0 : ]
            week_yaw_num[ key ] , week_query_num[ key ] = week_zero_list[ 0 : ] , week_zero_list[ 0 : ]
            yaw_all_num_qt[ key ] , query_all_num_qt[ key ] = 0 , 0 
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[3] ) )
        yaw_num , query_num = string.atoi( qr[5] ) , string.atoi( qr[4] )
        hour_yaw_num[ key ][ hour_idx - hour_st ]   += yaw_num
        hour_query_num[ key ][ hour_idx - hour_st ] += query_num
        day_yaw_num[ key ][ day_idx - day_st ]      += yaw_num
        day_query_num[ key ][ day_idx - day_st ]    += query_num
        week_yaw_num[ key ][ week_idx - week_st ]   += yaw_num
        week_query_num[ key ][ week_idx - week_st ] += query_num
        hour_yaw_all_num[ hour_idx - hour_st ]      += yaw_num
        day_yaw_all_num[ day_idx - day_st ]         += yaw_num
        week_yaw_all_num[ week_idx - week_st ]      += yaw_num
        yaw_all_num_qt[ key ]   += yaw_num
        query_all_num_qt[ key ] += query_num
        yaw_all_num_one_port    += yaw_num
    for key in name_dict:
        res[ "name_list" ].append( key )
        res[ "hour_yaw_num" ].append( hour_yaw_num[ key ] )
        res[ "hour_yaw_ratio" ].append( common_method.getListDivide( hour_yaw_num[ key ] , hour_query_num[ key ] , 100.0 ) )
        res[ "hour_yaw_percent" ].append( common_method.getListDivide( hour_yaw_num[ key ] , hour_yaw_all_num , 100.0 ) )
        res[ "day_yaw_num" ].append( day_yaw_num[ key ] )
        res[ "day_yaw_ratio" ].append( common_method.getListDivide( day_yaw_num[ key ] , day_query_num[ key ] , 100.0 ) )
        res[ "day_yaw_percent" ].append( common_method.getListDivide( day_yaw_num[ key ] , day_yaw_all_num , 100.0 ) )
        res[ "week_yaw_num" ].append( week_yaw_num[ key ] )
        res[ "week_yaw_ratio" ].append( common_method.getListDivide( week_yaw_num[ key ] , week_query_num[ key ] , 100.0 ) )
        res[ "week_yaw_percent" ].append( common_method.getListDivide( week_yaw_num[ key ] , week_yaw_all_num , 100.0 ) )
        res[ "all_yaw_ratio" ].append( yaw_all_num_qt[ key ] * 100.0 / query_all_num_qt[ key ] if query_all_num_qt[ key ] > 0 else 0 )
        res[ "all_yaw_percent" ].append( yaw_all_num_qt[ key ] * 100.0 / yaw_all_num_one_port if yaw_all_num_one_port > 0 else 0 )
    return res    
    
    
    
def query_port_yawpos_type_info_summary( st_time , ed_time , port_name ) :
    res = {}
    res[ "data_len" ]   = 0
    res[ "name_list" ]  = []
    res[ "hour_range" ] = []
    res[ "day_range" ]  = []
    res[ "week_range" ] = []
    res[ "all_count_list" ]  = [] 
    res[ "hour_count_list" ] = []   
    res[ "day_count_list" ]  = []   
    res[ "week_count_list" ] = []   
    res[ "hour_yaw_percent" ] = []   
    res[ "day_yaw_percent" ]  = []   
    res[ "week_yaw_percent" ] = []  
    res[ "hour_yaw_ratio" ] = []   
    res[ "day_yaw_ratio" ]  = []   
    res[ "week_yaw_ratio" ] = []
    
    cmd = "select port_name , time , start_n , center_n , end_n , num from " + sum_dict[ "yaw_time_info_extern" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    cmd = cmd + " and port_name=\"" + port_name + "\""
    #print port_name
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[1] , y[1] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
        
    if len( qres ) <=0 :
        return res
            
    res[ "data_len" ]   = len( qres )    
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 1 ] , qres[ -1 ][ 1 ] )
    
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    
    
    name_list = [ "start_yaw" , "center_yaw" , "end_yaw" ]
    hour_yaw_dict , day_yaw_dict , week_yaw_dict , all_yaw_dict = {} , {} , {} , {} 
    hour_count , day_count , week_count = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    hour_all_yaw , day_all_yaw , week_all_yaw = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    for key in name_list:
        hour_yaw_dict[ key ]   = hour_zero_list[0:]
        day_yaw_dict[ key ]    = day_zero_list[0:]
        week_yaw_dict[ key ]   = week_zero_list[0:]
        all_yaw_dict[ key ]    = 0
    
    for qr in qres:
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[1] ) )
        hour_yaw_dict[ "start_yaw" ][ hour_idx - hour_st ]    += string.atoi( qr[2] )
        day_yaw_dict[ "start_yaw" ][ day_idx - day_st ]       += string.atoi( qr[2] )
        week_yaw_dict[ "start_yaw" ][ week_idx - week_st ]    += string.atoi( qr[2] )
        hour_yaw_dict[ "center_yaw" ][ hour_idx - hour_st ]    += string.atoi( qr[3] )
        day_yaw_dict[ "center_yaw" ][ day_idx - day_st ]       += string.atoi( qr[3] )
        week_yaw_dict[ "center_yaw" ][ week_idx - week_st ]    += string.atoi( qr[3] )
        hour_yaw_dict[ "end_yaw" ][ hour_idx - hour_st ]    += string.atoi( qr[4] )
        day_yaw_dict[ "end_yaw" ][ day_idx - day_st ]       += string.atoi( qr[4] )
        week_yaw_dict[ "end_yaw" ][ week_idx - week_st ]    += string.atoi( qr[4] )
        all_yaw_dict[ "start_yaw" ]   += string.atoi( qr[2] )
        all_yaw_dict[ "center_yaw" ]  += string.atoi( qr[3] )
        all_yaw_dict[ "end_yaw" ]     += string.atoi( qr[4] )
        
        hour_all_yaw[ hour_idx - hour_st ]  += string.atoi( qr[2] ) + string.atoi( qr[3] ) + string.atoi( qr[4] )
        day_all_yaw[ day_idx - day_st ]     += string.atoi( qr[2] ) + string.atoi( qr[3] ) + string.atoi( qr[4] )
        week_all_yaw[ week_idx - week_st ]  += string.atoi( qr[2] ) + string.atoi( qr[3] ) + string.atoi( qr[4] )
        hour_count[ hour_idx - hour_st ]    += string.atoi( qr[5] )
        day_count[ day_idx - day_st ]       += string.atoi( qr[5] )
        week_count[ week_idx - week_st ]    += string.atoi( qr[5] )
         
    for key in name_list:
        res[ "name_list" ].append( key )
        res[ "all_count_list" ].append( all_yaw_dict[ key ] )
        res[ "hour_count_list" ].append( hour_yaw_dict[ key ] )
        res[ "day_count_list" ].append( day_yaw_dict[ key ] )
        res[ "week_count_list" ].append( week_yaw_dict[ key ] )
        res[ "hour_yaw_percent" ].append( common_method.getListDivide( hour_yaw_dict[ key ] , hour_all_yaw , 100.0 ) )
        res[ "day_yaw_percent" ].append( common_method.getListDivide( day_yaw_dict[ key ] , day_all_yaw , 100.0 ) )
        res[ "week_yaw_percent" ].append( common_method.getListDivide( week_yaw_dict[ key ] , week_all_yaw , 100.0 ) )
        res[ "hour_yaw_ratio" ].append( common_method.getListDivide( hour_yaw_dict[ key ] , hour_count , 100.0 ) )
        res[ "day_yaw_ratio" ].append( common_method.getListDivide( day_yaw_dict[ key ] , day_count , 100.0 ) )
        res[ "week_yaw_ratio" ].append( common_method.getListDivide( week_yaw_dict[ key ] , week_count , 100.0 ) )
        
    return res
    
    
    
    

#hour
#select qt, version, time, query_num from qt_version_time_summary where time <=b and time >=a
def query_version_pv( st_time , ed_time ) :
    global conn
    global cur
    global sum_dict
    cmd = "select qt , version , time , query_num , illegal_num from " + sum_dict[ "qt_version_time" ] ;
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    print cmd
    
    res = {}
    res[ "data_len" ]       = 0
    res[ "name_list" ]      = []
    
    res[ "hour_range" ]     = []
    res[ "hour_data_list" ] = []
    res[ "hour_ratio_list" ]= []
    res[ "hour_total_pv" ]  = []
    
    res[ "day_range" ]      = []
    res[ "day_data_list" ]  = []
    res[ "day_ratio_list" ] = []
    res[ "day_total_pv" ]   = []
    
    res[ "week_range" ]     = []
    res[ "week_data_list" ] = []
    res[ "week_ratio_list" ]= []
    res[ "week_total_pv" ]  = []
    
    res[ "all_range" ]      = []
    res[ "all_data_list" ]  = []
    res[ "all_ratio_list" ] = []
    
    res[ "day_week_data_list" ] = []
    res[ "week_day_data_list" ] = []
    res[ "day_week_total_pv" ]  = []
    res[ "week_day_total_pv" ]  = []
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[2] , y[2] ) )
    #print qres
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    #print qres
        
    if len( qres ) <=0 :
        return res
        
    res[ "data_len" ] = len( qres )
        
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 2 ] , qres[ -1 ][ 2 ] )
    
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    day_to_week_idx , week_day_count = day_zero_list[0:] , week_zero_list[0:]
    for i in range( len( day_to_week_idx ) ):
        day_to_week_idx[i] = common_method.UTChourToWeekIdx( (i+day_st)*24 ) - week_st
        week_day_count[ day_to_week_idx[i] ] += 1
    
    name_dict,hour_data_dict,day_data_dict,week_data_dict,all_data_dict = {},{},{},{},{}
    hour_data_all,day_data_all,week_data_all,all_data_all = hour_zero_list[ 0 : ] , day_zero_list[ 0 : ] , week_zero_list[ 0: ] , 0
            
    for qr in qres :
        #print qr
        key = qr[0] + "," + qr[1]
        if key not in name_dict:
            hour_data_dict[ key ]   = hour_zero_list[ 0 : ]
            day_data_dict[ key ]    = day_zero_list[ 0 : ]
            week_data_dict[ key ]   = week_zero_list[ 0 : ]
            all_data_dict[ key ]    = 0
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[2] ) )
        query_num = string.atoi( qr[3] ) + string.atoi( qr[4] )
        hour_data_dict[ key ][ hour_idx - hour_st ] += query_num
        day_data_dict[ key ][ day_idx - day_st ]    += query_num
        week_data_dict[ key ][ week_idx - week_st ] += query_num
        all_data_dict[ key ] += query_num
        hour_data_all[ hour_idx - hour_st ] += query_num
        day_data_all[ day_idx - day_st ]    += query_num
        week_data_all[ week_idx - week_st ] += query_num
        all_data_all += query_num
        name_dict[ key ] = 1
    for  k in name_dict :
        res[ "name_list" ].append( k )
        res[ "hour_data_list" ].append( hour_data_dict[k] )
        res[ "day_data_list" ].append( day_data_dict[k] )
        res[ "week_data_list" ].append( week_data_dict[k] )
        res[ "all_data_list" ].append( all_data_dict[k] )
        res[ "hour_ratio_list" ].append( common_method.getListDivide( hour_data_dict[k] , hour_data_all , 100.0 ) )
        res[ "day_ratio_list" ].append( common_method.getListDivide( day_data_dict[k] , day_data_all , 100.0 ) )
        res[ "week_ratio_list" ].append( common_method.getListDivide( week_data_dict[k] , week_data_all , 100.0 ) )
        res[ "all_ratio_list" ].append( all_data_dict[k] * 100.0 / all_data_all if all_data_all > 0 else 0 )
        res[ "day_week_data_list" ].append( common_method.getDayWeekAvg( res[ "day_data_list" ][-1] , day_to_week_idx , week_day_count ) )
        res[ "week_day_data_list" ].append( common_method.getWeekDayAvg( res[ "day_data_list" ][-1] , day_to_week_idx , week_day_count ) )
    res[ "hour_total_pv" ]  = hour_data_all
    res[ "day_total_pv" ]   = day_data_all
    res[ "week_total_pv" ]  = week_data_all
    res[ "day_week_total_pv" ] = common_method.getDayWeekAvg( day_data_all , day_to_week_idx , week_day_count )
    res[ "week_day_total_pv" ] = common_method.getWeekDayAvg( day_data_all , day_to_week_idx , week_day_count )
    return res



    
def query_version2port_pv( st_time , ed_time , qt , version ) :
    global conn
    global cur
    global sum_dict
    cmd = "select from_info , resid , time , query_num , illegal_num from " + sum_dict[ "qt_version_time" ] ;
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    cmd = cmd + " and qt=\"" + qt + "\" and version=" + version
    print cmd
    
    res = {}
    res[ "data_len" ]       = 0
    res[ "name_list" ]      = []
    
    res[ "hour_range" ]     = []
    res[ "hour_data_list" ] = []
    res[ "hour_ratio_list" ]= []
    
    res[ "day_range" ]      = []
    res[ "day_data_list" ]  = []
    res[ "day_ratio_list" ] = []
    
    res[ "week_range" ]     = []
    res[ "week_data_list" ] = []
    res[ "week_ratio_list" ]= []
    
    res[ "all_range" ]      = []
    res[ "all_data_list" ]  = []
    res[ "all_ratio_list" ] = []
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[2] , y[2] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
        
    if len( qres ) <=0 :
        return res
        
    res[ "data_len" ] = len( qres )
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 2 ] , qres[ -1 ][ 2 ] )
    
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    
    name_dict,hour_data_dict,day_data_dict,week_data_dict,all_data_dict = {},{},{},{},{}
    hour_data_all,day_data_all,week_data_all,all_data_all = hour_zero_list[ 0 : ] , day_zero_list[ 0 : ] , week_zero_list[ 0: ] , 0
            
    for qr in qres :
        key = from_resid_qt_version_to_port_name( qr[0] , qr[1] , qt , version )
        if key not in name_dict:
            hour_data_dict[ key ]   = hour_zero_list[ 0 : ]
            day_data_dict[ key ]    = day_zero_list[ 0 : ]
            week_data_dict[ key ]   = week_zero_list[ 0 : ]
            all_data_dict[ key ]    = 0
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[2] ) )
        query_num = string.atoi( qr[3] ) + string.atoi( qr[4] )
        hour_data_dict[ key ][ hour_idx - hour_st ] += query_num
        day_data_dict[ key ][ day_idx - day_st ]    += query_num
        week_data_dict[ key ][ week_idx - week_st ] += query_num
        all_data_dict[ key ] += query_num
        hour_data_all[ hour_idx - hour_st ] += query_num
        day_data_all[ day_idx - day_st ]    += query_num
        week_data_all[ week_idx - week_st ] += query_num
        all_data_all += query_num
        name_dict[ key ] = 1
    for  k in name_dict :
        res[ "name_list" ].append( k )
        res[ "hour_data_list" ].append( hour_data_dict[k] )
        res[ "day_data_list" ].append( day_data_dict[k] )
        res[ "week_data_list" ].append( week_data_dict[k] )
        res[ "all_data_list" ].append( all_data_dict[k] )
        res[ "hour_ratio_list" ].append( common_method.getListDivide( hour_data_dict[k] , hour_data_all , 100.0 ) )
        res[ "day_ratio_list" ].append( common_method.getListDivide( day_data_dict[k] , day_data_all , 100.0 ) )
        res[ "week_ratio_list" ].append( common_method.getListDivide( week_data_dict[k] , week_data_all , 100.0 ) )
        res[ "all_ratio_list" ].append( all_data_dict[k] * 100.0 / all_data_all if all_data_all > 0 else 0 )
    return res 
    
    
    
    

#hour
#select qt, version, time, query_num, err_num from qt_version_time_summary where time <=b and time >=a
def query_version_error( st_time , ed_time ) :
    global conn
    global cur
    global sum_dict
    cmd = "select qt , version , time , query_num , err_num from " + sum_dict[ "qt_version_time" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    
    res = {}
    res[ "data_len" ]       = 0
    res[ "name_list" ]      = []
    
    res[ "hour_range" ]        = []
    res[ "hour_err_ratio" ]    = []
    res[ "hour_err_percent" ]  = []
    res[ "hour_total_ratio" ]  = []
    
    res[ "day_range" ]         = []
    res[ "day_err_ratio" ]     = []
    res[ "day_err_percent" ]   = []
    res[ "day_total_ratio" ]   = []
    
    res[ "week_range" ]        = []
    res[ "week_err_ratio" ]    = []
    res[ "week_err_percent" ]  = []
    res[ "week_total_ratio" ]  = []

    res[ "all_err_ratio" ]     = []
    res[ "all_err_percent" ]   = []
    
    res[ "day_week_err_ratio" ]    = []
    res[ "day_week_err_percent" ]  = []
    res[ "day_week_total_ratio" ]  = []
    res[ "week_day_err_ratio" ]    = []
    res[ "week_day_err_percent" ]  = []
    res[ "week_day_total_ratio" ]  = []
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[2] , y[2] ) )
    
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    #print qres
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
    
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 2 ] , qres[ -1 ][ 2 ] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    day_to_week_idx , week_day_count = day_zero_list[0:] , week_zero_list[0:]
    for i in range( len( day_to_week_idx ) ):
        day_to_week_idx[i] = common_method.UTChourToWeekIdx( (i+day_st)*24 ) - week_st
        week_day_count[ day_to_week_idx[i] ] += 1
    
    name_dict       = {}
    hour_all_dict   = {}
    hour_err_dict   = {}
    hour_all_err    = hour_zero_list[ 0 : ]
    day_all_dict    = {}
    day_err_dict    = {}
    day_all_err     = day_zero_list[ 0 : ]
    week_all_dict   = {}
    week_err_dict   = {}
    week_all_err    = week_zero_list[ 0 : ]
    data_all_dict   = {}
    data_err_dict   = {}
    data_all_err    = 0
    
    hour_total_pv , day_total_pv , week_total_pv = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    hour_total_err , day_total_err , week_total_err = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    
    for qr in qres :
        key = qr[0] + "," + qr[1]
        if key not in name_dict:
            hour_all_dict[ key ]    = hour_zero_list[ 0 : ]
            hour_err_dict[ key ]    = hour_zero_list[ 0 : ]
            day_all_dict[ key ]     = day_zero_list[ 0 : ]
            day_err_dict[ key ]     = day_zero_list[ 0 : ]
            week_all_dict[ key ]    = week_zero_list[ 0 : ]
            week_err_dict[ key ]    = week_zero_list[ 0 : ]
            data_all_dict[ key ]    = 0
            data_err_dict[ key ]    = 0
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[2] ) )
        query_num , err_num = string.atoi( qr[3] ) , string.atoi( qr[4] )
        hour_all_dict[ key ][ hour_idx - hour_st ]  += query_num
        hour_err_dict[ key ][ hour_idx - hour_st ]  += err_num
        hour_all_err[ hour_idx - hour_st ]          += err_num
        day_all_dict[ key ][ day_idx - day_st ]     += query_num
        day_err_dict[ key ][ day_idx - day_st ]     += err_num
        day_all_err[ day_idx - day_st ]             += err_num
        week_all_dict[ key ][ week_idx - week_st ]  += query_num
        week_err_dict[ key ][ week_idx - week_st ]  += err_num
        week_all_err[ week_idx - week_st ]          += err_num
        data_all_dict[ key ] += query_num
        data_err_dict[ key ] += err_num
        hour_total_pv[ hour_idx - hour_st ] += query_num
        day_total_pv[ day_idx - day_st ]    += query_num
        week_total_pv[ week_idx - week_st ] += query_num
        hour_total_err[ hour_idx - hour_st ] += err_num
        day_total_err[ day_idx - day_st ]    += err_num
        week_total_err[ week_idx - week_st ] += err_num
        data_all_err         += err_num
        name_dict[ key ] = 1
    for  k in name_dict :
        res[ "name_list" ].append( k )
        res[ "hour_err_ratio" ].append( common_method.getListDivide( hour_err_dict[k] , hour_all_dict[k] , 100.0 ) )
        res[ "hour_err_percent" ].append( common_method.getListDivide( hour_err_dict[k] , hour_all_err , 100.0 ) )
        res[ "day_err_ratio" ].append( common_method.getListDivide( day_err_dict[k] , day_all_dict[k] , 100.0 ) )
        res[ "day_err_percent" ].append( common_method.getListDivide( day_err_dict[k] , day_all_err , 100.0 ) )
        res[ "week_err_ratio" ] .append( common_method.getListDivide( week_err_dict[k] , week_all_dict[k] , 100.0 ) )
        res[ "week_err_percent" ].append( common_method.getListDivide( week_err_dict[k] , week_all_err , 100.0 ) )
        res[ "all_err_ratio" ].append( data_err_dict[k] * 100.0 / data_all_dict[k] if data_all_dict[k] != 0 else 0 )
        res[ "all_err_percent" ].append( data_err_dict[k] * 100.0 / data_all_err if data_all_err != 0 else 0 )
        res[ "day_week_err_ratio" ].append( common_method.getDayWeekAvg( res["day_err_ratio"][-1], day_to_week_idx, week_day_count ) )
        res[ "day_week_err_percent" ].append( common_method.getDayWeekAvg( res["day_err_percent"][-1], day_to_week_idx, week_day_count ) )
        res[ "week_day_err_ratio" ].append( common_method.getWeekDayAvg( res["day_err_ratio"][-1], day_to_week_idx, week_day_count ) )
        res[ "week_day_err_percent" ].append( common_method.getWeekDayAvg( res["day_err_percent"][-1], day_to_week_idx, week_day_count ) )
    res[ "hour_total_ratio" ]  = common_method.getListDivide( hour_total_err , hour_total_pv , 100.0 )
    res[ "day_total_ratio" ]  = common_method.getListDivide( day_total_err , day_total_pv , 100.0 )
    res[ "week_total_ratio" ]  = common_method.getListDivide( week_total_err , week_total_pv , 100.0 )
    res[ "day_week_total_ratio" ]  = common_method.getDayWeekAvg( res["day_total_ratio"], day_to_week_idx, week_day_count )
    res[ "week_day_total_ratio" ]  = common_method.getWeekDayAvg( res["day_total_ratio"], day_to_week_idx, week_day_count )
    return res
    
    
    

def query_version2port_error( st_time , ed_time , qt , version ) :
    global conn
    global cur
    global sum_dict
    cmd = "select from_info , resid , time , query_num , err_num from " + sum_dict[ "qt_version_time" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    cmd = cmd + " and qt=\"" + qt + "\" and version=" + version
    
    res = {}
    res[ "data_len" ]       = 0
    res[ "name_list" ]      = []
    
    res[ "hour_range" ]        = []
    res[ "hour_err_ratio" ]    = []
    res[ "hour_err_percent" ]  = []
    
    res[ "day_range" ]         = []
    res[ "day_err_ratio" ]     = []
    res[ "day_err_percent" ]   = []
    
    res[ "week_range" ]        = []
    res[ "week_err_ratio" ]    = []
    res[ "week_err_percent" ]  = []

    res[ "all_err_ratio" ]     = []
    res[ "all_err_percent" ]   = []
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[2] , y[2] ) )
    
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    #print qres
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
    
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 2 ] , qres[ -1 ][ 2 ] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    
    name_dict       = {}
    hour_all_dict   = {}
    hour_err_dict   = {}
    hour_all_err    = hour_zero_list[ 0 : ]
    day_all_dict    = {}
    day_err_dict    = {}
    day_all_err     = day_zero_list[ 0 : ]
    week_all_dict   = {}
    week_err_dict   = {}
    week_all_err    = week_zero_list[ 0 : ]
    data_all_dict   = {}
    data_err_dict   = {}
    data_all_err    = 0
    
    for qr in qres :
        key = from_resid_qt_version_to_port_name( qr[0] , qr[1] , qt , version )
        if key not in name_dict:
            hour_all_dict[ key ]    = hour_zero_list[ 0 : ]
            hour_err_dict[ key ]    = hour_zero_list[ 0 : ]
            day_all_dict[ key ]     = day_zero_list[ 0 : ]
            day_err_dict[ key ]     = day_zero_list[ 0 : ]
            week_all_dict[ key ]    = week_zero_list[ 0 : ]
            week_err_dict[ key ]    = week_zero_list[ 0 : ]
            data_all_dict[ key ]    = 0
            data_err_dict[ key ]    = 0
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[2] ) )
        hour_all_dict[ key ][ hour_idx - hour_st ]  += string.atoi( qr[ 3 ] )
        hour_err_dict[ key ][ hour_idx - hour_st ]  += string.atoi( qr[ 4 ] )
        hour_all_err[ hour_idx - hour_st ]          += string.atoi( qr[ 4 ] )
        day_all_dict[ key ][ day_idx - day_st ]     += string.atoi( qr[ 3 ] )
        day_err_dict[ key ][ day_idx - day_st ]     += string.atoi( qr[ 4 ] )
        day_all_err[ day_idx - day_st ]             += string.atoi( qr[ 4 ] )
        week_all_dict[ key ][ week_idx - week_st ]  += string.atoi( qr[ 3 ] )
        week_err_dict[ key ][ week_idx - week_st ]  += string.atoi( qr[ 4 ] )
        week_all_err[ week_idx - week_st ]          += string.atoi( qr[ 4 ] )
        data_all_dict[ key ] += string.atoi( qr[ 3 ] )
        data_err_dict[ key ] += string.atoi( qr[ 4 ] )
        data_all_err         += string.atoi( qr[ 4 ] )
        name_dict[ key ] = 1
    for  k in name_dict :
        res[ "name_list" ].append( k )
        res[ "hour_err_ratio" ].append( common_method.getListDivide( hour_err_dict[k] , hour_all_dict[k] , 100.0 ) )
        res[ "hour_err_percent" ].append( common_method.getListDivide( hour_err_dict[k] , hour_all_err , 100.0 ) )
        res[ "day_err_ratio" ].append( common_method.getListDivide( day_err_dict[k] , day_all_dict[k] , 100.0 ) )
        res[ "day_err_percent" ].append( common_method.getListDivide( day_err_dict[k] , day_all_err , 100.0 ) )
        res[ "week_err_ratio" ] .append( common_method.getListDivide( week_err_dict[k] , week_all_dict[k] , 100.0 ) )
        res[ "week_err_percent" ].append( common_method.getListDivide( week_err_dict[k] , week_all_err , 100.0 ) )
        res[ "all_err_ratio" ].append( data_err_dict[k] * 100.0 / data_all_dict[k] if data_all_dict[k] != 0 else 0 )
        res[ "all_err_percent" ].append( data_err_dict[k] * 100.0 / data_all_err if data_all_err != 0 else 0 )
    return res    
    
    


#hour
#select qt, version, time, query_num, err_num from qt_version_time_summary where time <=b and time >=a
def query_version_illegal( st_time , ed_time ) :
    global conn
    global cur
    global sum_dict
    cmd = "select qt , version , time , query_num , illegal_num from " + sum_dict[ "qt_version_time" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    
    res = {}
    res[ "data_len" ]       = 0
    res[ "name_list" ]      = []
    
    res[ "hour_range" ]        = []
    res[ "hour_illegal_num" ]    = []
    res[ "hour_illegal_ratio" ]    = []
    res[ "hour_illegal_percent" ]  = []
    res[ "hour_total_illegal_num" ]  = []
    res[ "hour_total_illegal_ratio" ]  = []
    
    res[ "day_range" ]         = []
    res[ "day_illegal_num" ]     = []
    res[ "day_illegal_ratio" ]     = []
    res[ "day_illegal_percent" ]   = []
    res[ "day_total_illegal_num" ]   = []
    res[ "day_total_illegal_ratio" ]   = []
    
    res[ "week_range" ]        = []
    res[ "week_illegal_num" ]    = []
    res[ "week_illegal_ratio" ]    = []
    res[ "week_illegal_percent" ]  = []
    res[ "week_total_illegal_num" ]  = []
    res[ "week_total_illegal_ratio" ]  = []

    res[ "all_illegal_ratio" ]     = []
    res[ "all_illegal_percent" ]   = []
    
    res[ "day_week_illegal_ratio" ]    = []
    res[ "day_week_illegal_percent" ]  = []
    res[ "day_week_total_ratio" ]  = []
    res[ "week_day_illegal_ratio" ]    = []
    res[ "week_day_illegal_percent" ]  = []
    res[ "week_day_total_ratio" ]  = []
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[2] , y[2] ) )
    
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    #print qres
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
    
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 2 ] , qres[ -1 ][ 2 ] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    day_to_week_idx , week_day_count = day_zero_list[0:] , week_zero_list[0:]
    for i in range( len( day_to_week_idx ) ):
        day_to_week_idx[i] = common_method.UTChourToWeekIdx( (i+day_st)*24 ) - week_st
        week_day_count[ day_to_week_idx[i] ] += 1
    
    name_dict       = {}
    hour_all_dict   = {}
    hour_illegal_dict   = {}
    hour_all_illegal    = hour_zero_list[ 0 : ]
    day_all_dict    = {}
    day_illegal_dict    = {}
    day_all_illegal     = day_zero_list[ 0 : ]
    week_all_dict   = {}
    week_illegal_dict   = {}
    week_all_illegal    = week_zero_list[ 0 : ]
    data_all_dict   = {}
    data_illegal_dict   = {}
    data_all_illegal    = 0
    
    hour_total_pv , day_total_pv , week_total_pv = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    hour_total_illegal , day_total_illegal , week_total_illegal = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    
    for qr in qres :
        key = qr[0] + "," + qr[1]
        if key not in name_dict:
            hour_all_dict[ key ]    = hour_zero_list[ 0 : ]
            hour_illegal_dict[ key ]    = hour_zero_list[ 0 : ]
            day_all_dict[ key ]     = day_zero_list[ 0 : ]
            day_illegal_dict[ key ]     = day_zero_list[ 0 : ]
            week_all_dict[ key ]    = week_zero_list[ 0 : ]
            week_illegal_dict[ key ]    = week_zero_list[ 0 : ]
            data_all_dict[ key ]    = 0
            data_illegal_dict[ key ]    = 0
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[2] ) )
        query_num , illegal_num = string.atoi( qr[3] ) + string.atoi( qr[4] ), string.atoi( qr[4] )
        hour_all_dict[ key ][ hour_idx - hour_st ]  += query_num
        hour_illegal_dict[ key ][ hour_idx - hour_st ]  += illegal_num
        hour_all_illegal[ hour_idx - hour_st ]          += illegal_num
        day_all_dict[ key ][ day_idx - day_st ]     += query_num
        day_illegal_dict[ key ][ day_idx - day_st ]     += illegal_num
        day_all_illegal[ day_idx - day_st ]             += illegal_num
        week_all_dict[ key ][ week_idx - week_st ]  += query_num
        week_illegal_dict[ key ][ week_idx - week_st ]  += illegal_num
        week_all_illegal[ week_idx - week_st ]          += illegal_num
        data_all_dict[ key ] += query_num
        data_illegal_dict[ key ] += illegal_num
        hour_total_pv[ hour_idx - hour_st ] += query_num
        day_total_pv[ day_idx - day_st ]    += query_num
        week_total_pv[ week_idx - week_st ] += query_num
        hour_total_illegal[ hour_idx - hour_st ] += illegal_num
        day_total_illegal[ day_idx - day_st ]    += illegal_num
        week_total_illegal[ week_idx - week_st ] += illegal_num
        data_all_illegal         += illegal_num
        name_dict[ key ] = 1
    for  k in name_dict :
        res[ "name_list" ].append( k )
        res[ "hour_illegal_num" ].append( hour_illegal_dict[k] )
        res[ "hour_illegal_ratio" ].append( common_method.getListDivide( hour_illegal_dict[k] , hour_all_dict[k] , 100.0 ) )
        res[ "hour_illegal_percent" ].append( common_method.getListDivide( hour_illegal_dict[k] , hour_all_illegal , 100.0 ) )
        res[ "day_illegal_num" ].append( day_illegal_dict[k] )
        res[ "day_illegal_ratio" ].append( common_method.getListDivide( day_illegal_dict[k] , day_all_dict[k] , 100.0 ) )
        res[ "day_illegal_percent" ].append( common_method.getListDivide( day_illegal_dict[k] , day_all_illegal , 100.0 ) )
        res[ "week_illegal_num" ].append( week_illegal_dict[k] )
        res[ "week_illegal_ratio" ].append( common_method.getListDivide( week_illegal_dict[k] , week_all_dict[k] , 100.0 ) )
        res[ "week_illegal_percent" ].append( common_method.getListDivide( week_illegal_dict[k] , week_all_illegal , 100.0 ) )
        res[ "all_illegal_ratio" ].append( data_illegal_dict[k] * 100.0 / data_all_dict[k] if data_all_dict[k] != 0 else 0 )
        res[ "all_illegal_percent" ].append( data_illegal_dict[k] * 100.0 / data_all_illegal if data_all_illegal != 0 else 0 )
        res[ "day_week_illegal_ratio" ].append( common_method.getDayWeekAvg( res["day_illegal_ratio"][-1], day_to_week_idx, week_day_count ) )
        res[ "day_week_illegal_percent" ].append( common_method.getDayWeekAvg( res["day_illegal_percent"][-1], day_to_week_idx, week_day_count ) )
        res[ "week_day_illegal_ratio" ].append( common_method.getWeekDayAvg( res["day_illegal_ratio"][-1], day_to_week_idx, week_day_count ) )
        res[ "week_day_illegal_percent" ].append( common_method.getWeekDayAvg( res["day_illegal_percent"][-1], day_to_week_idx, week_day_count ) )
    res[ "hour_total_illegal_num" ]  = hour_total_illegal
    res[ "day_total_illegal_num" ]  = day_total_illegal
    res[ "week_total_illegal_num" ]  = week_total_illegal
    res[ "hour_total_illegal_ratio" ]  = common_method.getListDivide( hour_total_illegal , hour_total_pv , 100.0 )
    res[ "day_total_illegal_ratio" ]  = common_method.getListDivide( day_total_illegal , day_total_pv , 100.0 )
    res[ "week_total_illegal_ratio" ]  = common_method.getListDivide( week_total_illegal , week_total_pv , 100.0 )
    res[ "day_week_total_ratio" ]  = common_method.getDayWeekAvg( res["day_total_illegal_ratio"], day_to_week_idx, week_day_count )
    res[ "week_day_total_ratio" ]  = common_method.getWeekDayAvg( res["day_total_illegal_ratio"], day_to_week_idx, week_day_count )
    return res
    
    
    

def query_version2port_illegal( st_time , ed_time , qt , version ) :
    global conn
    global cur
    global sum_dict
    cmd = "select from_info , resid , time , query_num , illegal_num from " + sum_dict[ "qt_version_time" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    cmd = cmd + " and qt=\"" + qt + "\" and version=" + version
    
    res = {}
    res[ "data_len" ]       = 0
    res[ "name_list" ]      = []
    
    res[ "hour_range" ]        = []
    res[ "hour_illegal_num" ]    = []
    res[ "hour_illegal_ratio" ]    = []
    res[ "hour_illegal_percent" ]  = []
    
    res[ "day_range" ]         = []
    res[ "day_illegal_num" ]     = []
    res[ "day_illegal_ratio" ]     = []
    res[ "day_illegal_percent" ]   = []
    
    res[ "week_range" ]        = []
    res[ "week_illegal_num" ]    = []
    res[ "week_illegal_ratio" ]    = []
    res[ "week_illegal_percent" ]  = []

    res[ "all_illegal_ratio" ]     = []
    res[ "all_illegal_percent" ]   = []
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[2] , y[2] ) )
    
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    #print qres
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
    
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 2 ] , qres[ -1 ][ 2 ] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    
    name_dict       = {}
    hour_all_dict   = {}
    hour_illegal_dict   = {}
    hour_all_illegal    = hour_zero_list[ 0 : ]
    day_all_dict    = {}
    day_illegal_dict    = {}
    day_all_illegal     = day_zero_list[ 0 : ]
    week_all_dict   = {}
    week_illegal_dict   = {}
    week_all_illegal    = week_zero_list[ 0 : ]
    data_all_dict   = {}
    data_illegal_dict   = {}
    data_all_illegal    = 0
    
    for qr in qres :
        key = from_resid_qt_version_to_port_name( qr[0] , qr[1] , qt , version )
        if key not in name_dict:
            hour_all_dict[ key ]    = hour_zero_list[ 0 : ]
            hour_illegal_dict[ key ]    = hour_zero_list[ 0 : ]
            day_all_dict[ key ]     = day_zero_list[ 0 : ]
            day_illegal_dict[ key ]     = day_zero_list[ 0 : ]
            week_all_dict[ key ]    = week_zero_list[ 0 : ]
            week_illegal_dict[ key ]    = week_zero_list[ 0 : ]
            data_all_dict[ key ]    = 0
            data_illegal_dict[ key ]    = 0
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[2] ) )
        query_num , illegal_num = string.atoi( qr[ 3 ] ) + string.atoi( qr[ 4 ] ), string.atoi( qr[ 4 ] )
        hour_all_dict[ key ][ hour_idx - hour_st ]  += query_num
        hour_illegal_dict[ key ][ hour_idx - hour_st ]  += illegal_num
        hour_all_illegal[ hour_idx - hour_st ]          += illegal_num
        day_all_dict[ key ][ day_idx - day_st ]     += query_num
        day_illegal_dict[ key ][ day_idx - day_st ]     += illegal_num
        day_all_illegal[ day_idx - day_st ]             += illegal_num
        week_all_dict[ key ][ week_idx - week_st ]  += query_num
        week_illegal_dict[ key ][ week_idx - week_st ]  += illegal_num
        week_all_illegal[ week_idx - week_st ]          += illegal_num
        data_all_dict[ key ] += query_num
        data_illegal_dict[ key ] += illegal_num
        data_all_illegal         += illegal_num
        name_dict[ key ] = 1
    for  k in name_dict :
        res[ "name_list" ].append( k )
        res[ "hour_illegal_num" ].append( hour_illegal_dict[k] )
        res[ "hour_illegal_ratio" ].append( common_method.getListDivide( hour_illegal_dict[k] , hour_all_dict[k] , 100.0 ) )
        res[ "hour_illegal_percent" ].append( common_method.getListDivide( hour_illegal_dict[k] , hour_all_illegal , 100.0 ) )
        res[ "day_illegal_num" ].append( day_illegal_dict[k] )
        res[ "day_illegal_ratio" ].append( common_method.getListDivide( day_illegal_dict[k] , day_all_dict[k] , 100.0 ) )
        res[ "day_illegal_percent" ].append( common_method.getListDivide( day_illegal_dict[k] , day_all_illegal , 100.0 ) )
        res[ "week_illegal_num" ] .append( week_illegal_dict[k] )
        res[ "week_illegal_ratio" ] .append( common_method.getListDivide( week_illegal_dict[k] , week_all_dict[k] , 100.0 ) )
        res[ "week_illegal_percent" ].append( common_method.getListDivide( week_illegal_dict[k] , week_all_illegal , 100.0 ) )
        res[ "all_illegal_ratio" ].append( data_illegal_dict[k] * 100.0 / data_all_dict[k] if data_all_dict[k] != 0 else 0 )
        res[ "all_illegal_percent" ].append( data_illegal_dict[k] * 100.0 / data_all_illegal if data_all_illegal != 0 else 0 )
    return res    
    
        


#select qt, version, time, size , size_vector  from qt_version_time_summary where time <=b and time >=a
def query_version_data_size( st_time , ed_time ) :
    global conn
    global cur
    global sum_dict
    cmd = "select qt , version , time , size , size_vector , query_num from " + sum_dict[ "qt_version_time" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    
    data_vector = qvt_size_vec[0:]
    res = {}
    res[ "data_len" ]       = 0
    res[ "hour_range" ]     = []
    res[ "day_range" ]      = []
    res[ "week_range" ]     = []
    res[ "name_list" ]      = []
    res[ "hour_data_list" ] = []
    res[ "day_data_list" ]  = []
    res[ "week_data_list" ] = []
    res[ "all_data_list" ]  = []
    res[ "data_vector" ]    = data_vector
    res[ "data_div_list" ]  = []
    
    res[ "hour_total_data" ] = []
    res[ "day_total_data" ]  = []
    res[ "week_total_data" ] = []
    res[ "day_week_data_list" ] = []
    res[ "week_day_data_list" ] = []
    res[ "day_week_total_data" ] = []
    res[ "week_day_total_data" ] = []
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[2] , y[2] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    #print qres
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 2 ] , qres[ -1 ][ 2 ] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    size_zero_list  = [ 0 for i in range( 1 + len( data_vector ) ) ]
    day_to_week_idx , week_day_count = day_zero_list[0:] , week_zero_list[0:]
    for i in range( len( day_to_week_idx ) ):
        day_to_week_idx[i] = common_method.UTChourToWeekIdx( (i+day_st)*24 ) - week_st
        week_day_count[ day_to_week_idx[i] ] += 1
    
    name_dict       = {}
    hour_all_dict   = {}
    hour_num_dict   = {}
    day_all_dict    = {}
    day_num_dict    = {}
    week_all_dict   = {}
    week_num_dict   = {}
    data_all_dict   = {}
    data_num_dict   = {}
    data_div_dict   = {}
    hour_total_pv , day_total_pv , week_total_pv = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    hour_total_ds , day_total_ds , week_total_ds = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]        
    for qr in qres :
        key = qr[0] + "," + qr[1]
        if key not in name_dict:
            hour_all_dict[ key ]    = hour_zero_list[ 0 : ]
            hour_num_dict[ key ]    = hour_zero_list[ 0 : ]
            day_all_dict[ key ]     = day_zero_list[ 0 : ]
            day_num_dict[ key ]     = day_zero_list[ 0 : ]
            week_all_dict[ key ]    = week_zero_list[ 0 : ]
            week_num_dict[ key ]    = week_zero_list[ 0 : ]
            data_div_dict[ key ]    = size_zero_list[ 0 : ]
            data_all_dict[ key ]    = 0
            data_num_dict[ key ]    = 0
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[2] ) )
        total_size , query_num = string.atoi( qr[3] ) , string.atoi( qr[5] )
        hour_all_dict[ key ][ hour_idx - hour_st ]  += total_size
        hour_num_dict[ key ][ hour_idx - hour_st ]  += query_num
        day_all_dict[ key ][ day_idx - day_st ]     += total_size
        day_num_dict[ key ][ day_idx - day_st ]     += query_num
        week_all_dict[ key ][ week_idx - week_st ]  += total_size
        week_num_dict[ key ][ week_idx - week_st ]  += query_num
        data_all_dict[ key ]    += total_size
        data_num_dict[ key ]    += query_num
        hour_total_pv[ hour_idx - hour_st ] += query_num
        day_total_pv[ day_idx - day_st ]    += query_num
        week_total_pv[ week_idx - week_st ] += query_num
        hour_total_ds[ hour_idx - hour_st ] += total_size
        day_total_ds[ day_idx - day_st ]    += total_size
        week_total_ds[ week_idx - week_st ] += total_size
        one_div_list        = size_zero_list[ 0 : ]
        if len( qr[4] ) >= len( size_zero_list ) :
            one_div_list    = [ string.atoi( qrr ) for qrr in qr[4].split( '|' ) ] 
        data_div_dict[ key ]    = common_method.getListAdd( one_div_list , data_div_dict[ key ] )
        name_dict[ key ] = 1
    for  k in name_dict :
        query_num = common_method.getListSum( data_div_dict[ k ] )
        res[ "name_list" ].append( k )
        res[ "hour_data_list" ].append( common_method.getListDivide( hour_all_dict[ k ] , hour_num_dict[ k ] , 1.0 ) )
        res[ "day_data_list" ].append( common_method.getListDivide( day_all_dict[ k ] ,day_num_dict[ k ] , 1.0 ) )
        res[ "week_data_list" ].append( common_method.getListDivide( week_all_dict[ k ] ,week_num_dict[ k ] , 1.0 ) )
        res[ "all_data_list" ].append( data_all_dict[ k ] / data_num_dict[ k ] if data_num_dict[ k ] > 0 else 0 )
        res[ "data_div_list" ].append( [ ddd * 100.0 / query_num  if query_num > 0 else  0 for ddd in data_div_dict[ k ] ] )
        res[ "day_week_data_list" ].append( common_method.getDayWeekAvg( res[ "day_data_list" ][-1], day_to_week_idx , week_day_count ) )
        res[ "week_day_data_list" ].append( common_method.getWeekDayAvg( res[ "day_data_list" ][-1], day_to_week_idx , week_day_count ) )
    res[ "hour_total_data" ] = common_method.getListDivide( hour_total_ds , hour_total_pv , 1.0 ) 
    res[ "day_total_data" ]  = common_method.getListDivide( day_total_ds , day_total_pv , 1.0 ) 
    res[ "week_total_data" ] = common_method.getListDivide( week_total_ds , week_total_pv , 1.0 ) 
    res[ "day_week_total_data" ] = common_method.getDayWeekAvg( res[ "day_total_data" ], day_to_week_idx , week_day_count )
    res[ "week_day_total_data" ] = common_method.getWeekDayAvg( res[ "day_total_data" ], day_to_week_idx , week_day_count ) 
    return res
    
    
def query_version2port_data_size( st_time , ed_time , qt , version ) :
    global conn
    global cur
    global sum_dict
    cmd = "select from_info , resid , time , size , size_vector , query_num from " + sum_dict[ "qt_version_time" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    cmd = cmd + " and qt=\"" + qt + "\" and version=" + version 
    data_vector = qvt_size_vec[0:]
    
    res = {}
    res[ "data_len" ]       = 0
    res[ "hour_range" ]     = [];
    res[ "day_range" ]      = [];
    res[ "week_range" ]     = [];
    res[ "name_list" ]      = []
    res[ "hour_data_list" ] = []
    res[ "day_data_list" ]  = []
    res[ "week_data_list" ] = []
    res[ "all_data_list" ]  = []
    res[ "data_vector" ]    = data_vector
    res[ "data_div_list" ]  = []
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[2] , y[2] ) )
    
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    #print qres
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
    
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 2 ] , qres[ -1 ][ 2 ] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    size_zero_list  = [ 0 for i in range( 1 + len( data_vector ) ) ]
    
    name_dict       = {}
    hour_all_dict   = {}
    hour_num_dict   = {}
    day_all_dict    = {}
    day_num_dict    = {}
    week_all_dict   = {}
    week_num_dict   = {}
    data_all_dict   = {}
    data_num_dict   = {}
    data_div_dict   = {}
            
    for qr in qres :
        key = from_resid_qt_version_to_port_name( qr[0] , qr[1] , qt , version )
        if key not in name_dict:
            hour_all_dict[ key ]    = hour_zero_list[ 0 : ]
            hour_num_dict[ key ]    = hour_zero_list[ 0 : ]
            day_all_dict[ key ]     = day_zero_list[ 0 : ]
            day_num_dict[ key ]     = day_zero_list[ 0 : ]
            week_all_dict[ key ]    = week_zero_list[ 0 : ]
            week_num_dict[ key ]    = week_zero_list[ 0 : ]
            data_div_dict[ key ]    = size_zero_list[ 0 : ]
            data_all_dict[ key ]    = 0
            data_num_dict[ key ]    = 0
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[2] ) )
        total_size , query_num = string.atoi( qr[3] ) , string.atoi( qr[5] )
        hour_all_dict[ key ][ hour_idx - hour_st ]  += total_size
        hour_num_dict[ key ][ hour_idx - hour_st ]  += query_num
        day_all_dict[ key ][ day_idx - day_st ]     += total_size
        day_num_dict[ key ][ day_idx - day_st ]     += query_num
        week_all_dict[ key ][ week_idx - week_st ]  += total_size
        week_num_dict[ key ][ week_idx - week_st ]  += query_num
        data_all_dict[ key ]    += total_size
        data_num_dict[ key ]    += query_num
        one_div_list        = size_zero_list[ 0 : ]
        if len( qr[4] ) >= len( size_zero_list ) :
            one_div_list    = [ string.atoi( qrr ) for qrr in qr[4].split( '|' ) ] 
        data_div_dict[ key ]    = common_method.getListAdd( one_div_list , data_div_dict[ key ] )
        name_dict[ key ] = 1
    for  k in name_dict :
        query_num = common_method.getListSum( data_div_dict[ k ] )
        res[ "name_list" ].append( k )
        res[ "hour_data_list" ].append( common_method.getListDivide( hour_all_dict[ k ] , hour_num_dict[ k ] , 1.0 ) )
        res[ "day_data_list" ].append( common_method.getListDivide( day_all_dict[ k ] ,day_num_dict[ k ] , 1.0 ) )
        res[ "week_data_list" ].append( common_method.getListDivide( week_all_dict[ k ] ,week_num_dict[ k ] , 1.0 ) )
        res[ "all_data_list" ].append( data_all_dict[ k ] / data_num_dict[ k ] if data_num_dict[ k ] > 0 else 0 )
        res[ "data_div_list" ].append( [ ddd * 100.0 / query_num  if query_num > 0 else  0 for ddd in data_div_dict[ k ] ] )
    return res

   

    
    
    


#hour
#select qt , version , time , query_num , avg_time , time_vector  from qt_version_time_summary where time <=b and time >=a
def query_qt_version_time_distribution( st_time , ed_time ) :
    global conn
    global cur
    global sum_dict
    cmd = "select qt , version , time , query_num , avg_time , time_vector , overtime_num  from " + sum_dict[ "qt_version_time" ] ;
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    time_vector = qvt_time_vec[0:]
    
    res = {}
    res[ "data_len" ]       = 0
    res[ "name_list" ]      = []
    res[ "hour_range" ]     = []
    res[ "day_range" ]      = []
    res[ "week_range" ]     = []
    res[ "hour_data_list" ] = []
    res[ "day_data_list" ]  = []
    res[ "week_data_list" ] = []
    res[ "all_data_list" ]  = []
    res[ "data_vector" ]    = time_vector
    res[ "data_div_list" ]  = []
    res[ "data_div_num_list" ]  = []
    
    res[ "hour_ot_num_list" ] = []
    res[ "day_ot_num_list" ]  = []
    res[ "week_ot_num_list" ] = []
    res[ "hour_ot_ratio_list" ] = []
    res[ "day_ot_ratio_list" ]  = []
    res[ "week_ot_ratio_list" ] = []
    res[ "hour_total_ot_num_list" ] = []
    res[ "day_total_ot_num_list" ]  = []
    res[ "week_total_ot_num_list" ] = []
    res[ "hour_total_ot_ratio_list" ] = []
    res[ "day_total_ot_ratio_list" ]  = []
    res[ "week_total_ot_ratio_list" ] = []
    
    res[ "hour_total_avg_time" ] = []
    res[ "day_total_avg_time" ]  = []
    res[ "week_total_avg_time" ] = []
    
    res[ "day_week_data_list" ]  = []
    res[ "week_day_data_list" ]  = []
    res[ "day_week_total_avg_time" ]  = []
    res[ "week_day_total_avg_time" ]  = []
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[2] , y[2] ) )
    
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    #print qres
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
    
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 2 ] , qres[ -1 ][ 2 ] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    time_zero_list  = [ 0 for i in range( 1 + len( time_vector ) ) ]
    day_to_week_idx , week_day_count = day_zero_list[0:] , week_zero_list[0:]
    for i in range( len( day_to_week_idx ) ):
        day_to_week_idx[i] = common_method.UTChourToWeekIdx( (i+day_st)*24 ) - week_st
        week_day_count[ day_to_week_idx[i] ] += 1
    
    name_dict       = {}
    hour_all_time_d = {}
    hour_all_num_d  = {}
    hour_ot_num_d   = {}
    day_all_time_d  = {}
    day_all_num_d   = {}
    day_ot_num_d    = {}
    week_all_time_d = {}
    week_all_num_d  = {}
    week_ot_num_d   = {}
    data_all_time_d = {}
    data_all_num_d  = {}
    data_div_dict   = {}
    hour_total_pv , day_total_pv , week_total_pv = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    hour_total_time , day_total_time , week_total_time = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    hour_total_ot_num_list, day_total_ot_num_list, week_total_ot_num_list = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    
    for qr in qres :
        key = qr[0] + "," + qr[1]
        if key not in name_dict:
            name_dict[ key ]        = 1
            hour_all_time_d[ key ]  = hour_zero_list[ 0 : ]
            hour_all_num_d[ key ]   = hour_zero_list[ 0 : ]
            hour_ot_num_d[ key ]    = hour_zero_list[ 0 : ]
            day_all_time_d[ key ]   = day_zero_list[ 0 : ]
            day_all_num_d[ key ]    = day_zero_list[ 0 : ]
            day_ot_num_d[ key ]     = day_zero_list[ 0 : ]
            week_all_time_d[ key ]  = week_zero_list[ 0 : ]
            week_all_num_d[ key ]   = week_zero_list[ 0 : ]
            week_ot_num_d[ key ]    = week_zero_list[ 0 : ]
            data_div_dict[ key ]    = time_zero_list[ 0 : ]
            data_all_time_d[ key ]  = 0
            data_all_num_d[ key ]   = 0
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[2] ) )
        query_num , avg_time , ot_num = string.atof( qr[ 3 ] ) , string.atof( qr[ 4 ] ) , string.atoi( qr[6] )
        hour_all_time_d[ key ][ hour_idx - hour_st ]+= avg_time * query_num
        hour_all_num_d[ key ][ hour_idx - hour_st ] += query_num
        hour_ot_num_d[ key ][ hour_idx - hour_st ]  += ot_num
        day_all_time_d[ key ][ day_idx - day_st ]   += avg_time * query_num
        day_all_num_d[ key ][ day_idx - day_st ]    += query_num
        day_ot_num_d[ key ][ day_idx - day_st ]     += ot_num
        week_all_time_d[ key ][ week_idx - week_st ]+= avg_time * query_num
        week_all_num_d[ key ][ week_idx - week_st ] += query_num
        week_ot_num_d[ key ][ week_idx - week_st ]  += ot_num
        data_all_time_d[ key ]  += avg_time * query_num
        data_all_num_d[ key ]   += query_num
        hour_total_time[ hour_idx - hour_st ]   += avg_time * query_num
        day_total_time[ day_idx - day_st ]      += avg_time * query_num
        week_total_time[ week_idx - week_st ]   += avg_time * query_num
        hour_total_pv[ hour_idx - hour_st ]   += query_num
        day_total_pv[ day_idx - day_st ]      += query_num
        week_total_pv[ week_idx - week_st ]   += query_num
        hour_total_ot_num_list[ hour_idx - hour_st ]   += ot_num
        day_total_ot_num_list[ day_idx - day_st ]      += ot_num
        week_total_ot_num_list[ week_idx - week_st ]   += ot_num
        one_div_list        = time_zero_list[ 0 : ]
        if  qr[ 5 ]!="" :
            one_div_list    = [ string.atoi( qrr ) for qrr in qr[ 5 ].split( '|' ) ] 
            data_div_dict[ key ]    = common_method.getListAdd( one_div_list , data_div_dict[ key ] )
    for  k in name_dict :
        query_num       = common_method.getListSum( data_div_dict[ k ] )
        res[ "name_list" ].append( k )
        res[ "hour_data_list" ].append( common_method.getListDivide( hour_all_time_d[ k ] , hour_all_num_d[k] , 1.0 ) )
        res[ "day_data_list" ].append( common_method.getListDivide( day_all_time_d[ k ] , day_all_num_d[k] , 1.0 ) )
        res[ "week_data_list" ].append( common_method.getListDivide( week_all_time_d[ k ] , week_all_num_d[k] , 1.0 ) )
        res[ "all_data_list" ].append( data_all_time_d[ k ] / data_all_num_d[ k ] if data_all_num_d[ k ]>0 else 0 )
        res[ "data_div_list" ].append( [ ddd * 100.0 / query_num  if query_num > 0 else  0  for ddd in data_div_dict[ k ] ] )  
        res[ "data_div_num_list" ].append( data_div_dict[ k ] )
        res[ "hour_ot_num_list" ].append( hour_ot_num_d[ k ] )
        res[ "hour_ot_ratio_list" ].append( common_method.getListDivide( hour_ot_num_d[ k ] , hour_all_num_d[k] , 100.0 ) )
        res[ "day_ot_num_list" ].append( day_ot_num_d[ k ] )
        res[ "day_ot_ratio_list" ].append( common_method.getListDivide( day_ot_num_d[ k ] , day_all_num_d[k] , 100.0 ) )
        res[ "week_ot_num_list" ].append( week_ot_num_d[ k ] )
        res[ "week_ot_ratio_list" ].append( common_method.getListDivide( week_ot_num_d[ k ] , week_all_num_d[k] , 100.0 ) )
        res[ "day_week_data_list" ].append( common_method.getDayWeekAvg( res[ "day_data_list" ][-1] , day_to_week_idx , week_day_count ) )
        res[ "week_day_data_list" ].append( common_method.getWeekDayAvg( res[ "day_data_list" ][-1] , day_to_week_idx , week_day_count ) )
    res[ "hour_total_avg_time" ] = common_method.getListDivide( hour_total_time , hour_total_pv , 1.0 )
    res[ "day_total_avg_time" ]  = common_method.getListDivide( day_total_time , day_total_pv , 1.0 )
    res[ "week_total_avg_time" ] = common_method.getListDivide( week_total_time , week_total_pv , 1.0 )
    res[ "day_week_total_avg_time" ]  = common_method.getDayWeekAvg( res[ "day_total_avg_time" ] , day_to_week_idx , week_day_count )
    res[ "week_day_total_avg_time" ]  = common_method.getWeekDayAvg( res[ "day_total_avg_time" ] , day_to_week_idx , week_day_count )
    res[ "hour_total_ot_num_list" ] = hour_total_ot_num_list
    res[ "day_total_ot_num_list" ]  = day_total_ot_num_list
    res[ "week_total_ot_num_list" ] = week_total_ot_num_list
    res[ "hour_total_ot_ratio_list" ] = common_method.getListDivide( hour_total_ot_num_list , hour_total_pv , 100.0 )
    res[ "day_total_ot_ratio_list" ]  = common_method.getListDivide( day_total_ot_num_list , day_total_pv , 100.0 )
    res[ "week_total_ot_ratio_list" ] = common_method.getListDivide( week_total_ot_num_list , week_total_pv , 100.0 )
    return res
    
    
#hour
#select qt , version , module , time , query_num , avg_time , time_vector  from qt_version_time_module_summary where time <=b and time >=a
def query_qt_version2module_time_distribution( st_time , ed_time , qt , version ) :
    global conn
    global cur
    global sum_dict
    cmd = "select qt , module , time , query_num , avg_time , time_vector , overtime_num  from " + sum_dict[ "qt_version_time_module" ] ;
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    if qt != "":
        cmd = cmd + " and qt=\"" + qt + "\""
        if version != "":
            cmd = cmd + " and version=" + version
    time_vector = qvtm_time_vec[0:]
    print cmd 
    res = {}
    res[ "data_len" ]       = 0
    res[ "name_list" ]      = []
    res[ "hour_range" ]     = []
    res[ "day_range" ]      = []
    res[ "week_range" ]     = []
    res[ "hour_data_list" ] = []
    res[ "day_data_list" ]  = []
    res[ "week_data_list" ] = []
    res[ "all_data_list" ]  = []
    res[ "data_vector" ]    = time_vector
    res[ "data_div_list" ]  = []
    res[ "data_div_num_list" ] = []
    
    res[ "hour_ot_num_list" ] = []
    res[ "day_ot_num_list" ]  = []
    res[ "week_ot_num_list" ] = []
    res[ "hour_ot_ratio_list" ] = []
    res[ "day_ot_ratio_list" ]  = []
    res[ "week_ot_ratio_list" ] = []
    
    res[ "hour_total_ot_num_list" ] = []
    res[ "day_total_ot_num_list" ]  = []
    res[ "week_total_ot_num_list" ] = []
    res[ "hour_total_ot_ratio_list" ] = []
    res[ "day_total_ot_ratio_list" ]  = []
    res[ "week_total_ot_ratio_list" ] = []
    
    res[ "day_week_data_list" ]  = []
    res[ "week_day_data_list" ]  = []
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[2] , y[2] ) )
    
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    #print qres
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
    
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 2 ] , qres[ -1 ][ 2 ] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    time_zero_list  = [ 0 for i in range( 1 + len( time_vector ) ) ]
    day_to_week_idx , week_day_count = day_zero_list[0:] , week_zero_list[0:]
    for i in range( len( day_to_week_idx ) ):
        day_to_week_idx[i] = common_method.UTChourToWeekIdx( (i+day_st)*24 ) - week_st
        week_day_count[ day_to_week_idx[i] ] += 1
    
    name_dict       = {}
    hour_all_time_d = {}
    hour_all_num_d  = {}
    hour_ot_num_d   = {}
    day_all_time_d  = {}
    day_all_num_d   = {}
    day_ot_num_d    = {}
    week_all_time_d = {}
    week_all_num_d  = {}
    week_ot_num_d   = {}
    data_all_time_d = {}
    data_all_num_d  = {}
    data_div_dict   = {}
    hour_total_ot_num_list , day_total_ot_num_list , week_total_ot_num_list = hour_zero_list[0:], day_zero_list[0:], week_zero_list[0:]
    hour_total_pv_num_list , day_total_pv_num_list , week_total_pv_num_list = hour_zero_list[0:], day_zero_list[0:], week_zero_list[0:]
    
    for qr in qres :
        key = qr[1]
        if key == "total_time":
            continue
        if key in module_show_name_dict:
            key = module_show_name_dict[ key ]
        if key not in name_dict:
            name_dict[ key ]        = 1
            hour_all_time_d[ key ]  = hour_zero_list[ 0 : ]
            hour_all_num_d[ key ]   = hour_zero_list[ 0 : ]
            hour_ot_num_d[ key ]    = hour_zero_list[ 0 : ]
            day_all_time_d[ key ]   = day_zero_list[ 0 : ]
            day_all_num_d[ key ]    = day_zero_list[ 0 : ]
            day_ot_num_d[ key ]     = day_zero_list[ 0 : ]
            week_all_time_d[ key ]  = week_zero_list[ 0 : ]
            week_all_num_d[ key ]   = week_zero_list[ 0 : ]
            week_ot_num_d[ key ]    = week_zero_list[ 0 : ]
            data_div_dict[ key ]    = time_zero_list[ 0 : ]
            data_all_time_d[ key ]  = 0
            data_all_num_d[ key ]   = 0
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[2] ) )
        query_num , avg_time , ot_num = string.atof( qr[ 3 ] ) , string.atof( qr[ 4 ] ) , string.atoi( qr[6] )
        hour_all_time_d[ key ][ hour_idx - hour_st ]+= avg_time * query_num
        hour_all_num_d[ key ][ hour_idx - hour_st ] += query_num
        hour_ot_num_d[ key ][ hour_idx - hour_st ]  += ot_num
        day_all_time_d[ key ][ day_idx - day_st ]   += avg_time * query_num
        day_all_num_d[ key ][ day_idx - day_st ]    += query_num
        day_ot_num_d[ key ][ day_idx - day_st ]     += ot_num
        week_all_time_d[ key ][ week_idx - week_st ]+= avg_time * query_num
        week_all_num_d[ key ][ week_idx - week_st ] += query_num
        week_ot_num_d[ key ][ week_idx - week_st ]  += ot_num
        data_all_time_d[ key ]  += avg_time * query_num
        data_all_num_d[ key ]   += query_num        
        hour_total_ot_num_list[ hour_idx - hour_st ] += ot_num
        day_total_ot_num_list[ day_idx - day_st ]    += ot_num
        week_total_ot_num_list[ week_idx - week_st ] += ot_num
        hour_total_pv_num_list[ hour_idx - hour_st ] += query_num
        day_total_pv_num_list[ day_idx - day_st ]    += query_num
        week_total_pv_num_list[ week_idx - week_st ] += query_num
        
        one_div_list        = time_zero_list[ 0 : ]
        if len( qr[ 5 ] ) >= len( time_zero_list ) :
            one_div_list    = [ string.atoi( qrr ) for qrr in qr[ 5 ].split( '|' ) ] 
            data_div_dict[ key ]    = common_method.getListAdd( one_div_list , data_div_dict[ key ] )
    for  k in name_dict :
        query_num       = common_method.getListSum( data_div_dict[ k ] )
        res[ "name_list" ].append( k )
        res[ "hour_data_list" ].append( common_method.getListDivide( hour_all_time_d[ k ] , hour_all_num_d[k] , 1.0 ) )
        res[ "day_data_list" ].append( common_method.getListDivide( day_all_time_d[ k ] , day_all_num_d[k] , 1.0 ) )
        res[ "week_data_list" ].append( common_method.getListDivide( week_all_time_d[ k ] , week_all_num_d[k] , 1.0 ) )
        res[ "all_data_list" ].append( data_all_time_d[ k ] / data_all_num_d[ k ] if data_all_num_d[ k ]>0 else 0 )
        res[ "data_div_list" ].append( [ ddd * 100.0 / query_num  if query_num > 0 else  0  for ddd in data_div_dict[ k ] ] )  
        res[ "data_div_num_list" ].append( data_div_dict[ k ] )
        res[ "hour_ot_num_list" ].append( hour_ot_num_d[ k ] )
        res[ "hour_ot_ratio_list" ].append( common_method.getListDivide( hour_ot_num_d[ k ] , hour_all_num_d[k] , 100.0 ) )
        res[ "day_ot_num_list" ].append( day_ot_num_d[ k ] )
        res[ "day_ot_ratio_list" ].append( common_method.getListDivide( day_ot_num_d[ k ] , day_all_num_d[k] , 100.0 ) )
        res[ "week_ot_num_list" ].append( week_ot_num_d[ k ] )
        res[ "week_ot_ratio_list" ].append( common_method.getListDivide( week_ot_num_d[ k ] , week_all_num_d[k] , 100.0 ) )
        res[ "day_week_data_list" ].append( common_method.getDayWeekAvg( res[ "day_data_list" ][-1] , day_to_week_idx , week_day_count ) )
        res[ "week_day_data_list" ].append( common_method.getWeekDayAvg( res[ "day_data_list" ][-1] , day_to_week_idx , week_day_count ) )
    res[ "hour_total_ot_num_list" ] = hour_total_ot_num_list
    res[ "day_total_ot_num_list" ]  = day_total_ot_num_list
    res[ "week_total_ot_num_list" ] = week_total_ot_num_list
    res[ "hour_total_ot_ratio_list" ] = common_method.getListDivide( hour_total_ot_num_list , hour_total_pv_num_list , 100.0 )
    res[ "day_total_ot_ratio_list" ]  = common_method.getListDivide( day_total_ot_num_list , day_total_pv_num_list, 100.0 )
    res[ "week_total_ot_ratio_list" ] = common_method.getListDivide( week_total_ot_num_list , week_total_pv_num_list , 100.0 )
    return res

    
    
def query_qt_version2port_time_distribution( st_time , ed_time , qt , version ) :
    global conn
    global cur
    global sum_dict
    cmd = "select from_info , resid , time , query_num , avg_time , time_vector , overtime_num  from " + sum_dict[ "qt_version_time" ] ;
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    cmd = cmd + " and qt=\"" + qt + "\" and version=" + version
    time_vector = qvt_time_vec[0:]
    
    res = {}
    res[ "data_len" ]       = 0
    res[ "name_list" ]      = []
    res[ "hour_range" ]     = []
    res[ "day_range" ]      = []
    res[ "week_range" ]     = []
    res[ "hour_data_list" ] = []
    res[ "day_data_list" ]  = []
    res[ "week_data_list" ] = []
    res[ "all_data_list" ]  = []
    res[ "data_vector" ]    = time_vector
    res[ "data_div_list" ]  = []
    res[ "data_div_num_list" ] = []
    
    res[ "hour_ot_num_list" ] = []
    res[ "day_ot_num_list" ]  = []
    res[ "week_ot_num_list" ] = []
    res[ "hour_ot_ratio_list" ] = []
    res[ "day_ot_ratio_list" ]  = []
    res[ "week_ot_ratio_list" ] = []
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[2] , y[2] ) )
    
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    #print qres
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
    
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 2 ] , qres[ -1 ][ 2 ] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    time_zero_list  = [ 0 for i in range( 1 + len( time_vector ) ) ]
    
    name_dict       = {}
    hour_all_time_d = {}
    hour_all_num_d  = {}
    hour_ot_num_d   = {}
    day_all_time_d  = {}
    day_all_num_d   = {}
    day_ot_num_d    = {}
    week_all_time_d = {}
    week_all_num_d  = {}
    week_ot_num_d   = {}
    data_all_time_d = {}
    data_all_num_d  = {}
    data_div_dict   = {}
            
    for qr in qres :
        key = from_resid_qt_version_to_port_name( qr[0] , qr[1] , qt , version )
        if key not in name_dict:
            name_dict[ key ]        = 1
            hour_all_time_d[ key ]  = hour_zero_list[ 0 : ]
            hour_all_num_d[ key ]   = hour_zero_list[ 0 : ]
            hour_ot_num_d[ key ]    = hour_zero_list[ 0 : ]
            day_all_time_d[ key ]   = day_zero_list[ 0 : ]
            day_all_num_d[ key ]    = day_zero_list[ 0 : ]
            day_ot_num_d[ key ]     = day_zero_list[ 0 : ]
            week_all_time_d[ key ]  = week_zero_list[ 0 : ]
            week_all_num_d[ key ]   = week_zero_list[ 0 : ]
            week_ot_num_d[ key ]    = week_zero_list[ 0 : ]
            data_div_dict[ key ]    = time_zero_list[ 0 : ]
            data_all_time_d[ key ]  = 0
            data_all_num_d[ key ]   = 0
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[2] ) )
        query_num , avg_time , ot_num = string.atof( qr[ 3 ] ) , string.atof( qr[ 4 ] ) , string.atoi( qr[6] )
        hour_all_time_d[ key ][ hour_idx - hour_st ]+= avg_time * query_num
        hour_all_num_d[ key ][ hour_idx - hour_st ] += query_num
        hour_ot_num_d[ key ][ hour_idx - hour_st ]  += ot_num
        day_all_time_d[ key ][ day_idx - day_st ]   += avg_time * query_num
        day_all_num_d[ key ][ day_idx - day_st ]    += query_num
        day_ot_num_d[ key ][ day_idx - day_st ]     += ot_num
        week_all_time_d[ key ][ week_idx - week_st ]+= avg_time * query_num
        week_all_num_d[ key ][ week_idx - week_st ] += query_num
        week_ot_num_d[ key ][ week_idx - week_st ]  += ot_num
        data_all_time_d[ key ]  += avg_time * query_num
        data_all_num_d[ key ]   += query_num
        one_div_list        = time_zero_list[ 0 : ]
        if qr[ 5 ]!= "" :
            one_div_list    = [ string.atoi( qrr ) for qrr in qr[ 5 ].split( '|' ) ] 
            data_div_dict[ key ]    = common_method.getListAdd( one_div_list , data_div_dict[ key ] )
    for  k in name_dict :
        query_num       = common_method.getListSum( data_div_dict[ k ] )
        res[ "name_list" ].append( k )
        res[ "hour_data_list" ].append( common_method.getListDivide( hour_all_time_d[ k ] , hour_all_num_d[k] , 1.0 ) )
        res[ "day_data_list" ].append( common_method.getListDivide( day_all_time_d[ k ] , day_all_num_d[k] , 1.0 ) )
        res[ "week_data_list" ].append( common_method.getListDivide( week_all_time_d[ k ] , week_all_num_d[k] , 1.0 ) )
        res[ "all_data_list" ].append( data_all_time_d[ k ] / data_all_num_d[ k ] if data_all_num_d[ k ]>0 else 0 )
        res[ "data_div_list" ].append( [ ddd * 100.0 / query_num  if query_num > 0 else  0  for ddd in data_div_dict[ k ] ] )  
        res[ "data_div_num_list" ].append( data_div_dict[ k ] )
        res[ "hour_ot_num_list" ].append( hour_ot_num_d[ k ] )
        res[ "hour_ot_ratio_list" ].append( common_method.getListDivide( hour_ot_num_d[ k ] , hour_all_num_d[k] , 100.0 ) )
        res[ "day_ot_num_list" ].append( day_ot_num_d[ k ] )
        res[ "day_ot_ratio_list" ].append( common_method.getListDivide( day_ot_num_d[ k ] , day_all_num_d[k] , 100.0 ) )
        res[ "week_ot_num_list" ].append( week_ot_num_d[ k ] )
        res[ "week_ot_ratio_list" ].append( common_method.getListDivide( week_ot_num_d[ k ] , week_all_num_d[k] , 100.0 ) )
    return res
       

def query_multinavi_session_uv( st_time , ed_time ):
    global conn
    global cur
    global sum_dict
    res = {}
    res[ "data_len" ]       = 0
    res[ "name_list" ]      = []
    res[ "day_range" ]      = []
    res[ "week_range" ]     = []
    res[ "day_uv_list" ]      = []
    res[ "week_uv_list" ]     = []
    res[ "total_uv_list" ]    = []
    res[ "day_uv_percent" ]      = []
    res[ "week_uv_percent" ]     = []
    res[ "total_uv_percent" ]    = []
    
    cmd = "select resid , version , time , uv from " + sum_dict[ "resid_cuid_time_pv" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[2] , y[2] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[0][2] , qres[-1][2] )
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    name_dict       = {}
    day_uv_dict , week_uv_dict , total_uv_dict = {} , {} , {}
    day_uv_total , week_uv_total , total_uv_total = day_zero_list[0:] , week_zero_list[0:] , 0
    
    for qr in qres :
        key = qr[0]+"|"+qr[1]
        if key not in name_dict:
            name_dict[ key ]        = 1
            day_uv_dict[ key ]      = day_zero_list[0:]
            week_uv_dict[ key ]     = week_zero_list[0:] 
            total_uv_dict[ key ]    = 0
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[2] ) )
        query_num = string.atoi( qr[3] )
        day_uv_dict[ key ][ day_idx - day_st ]    += query_num
        week_uv_dict[ key ][ week_idx - week_st ] += query_num
        total_uv_dict[ key ]   += query_num
        day_uv_total[ day_idx - day_st ]    += query_num
        week_uv_total[ week_idx - week_st ] += query_num
        total_uv_total += query_num
        
    for  key in name_dict :
        res[ "name_list" ].append( key )
        res[ "day_uv_list" ].append( day_uv_dict[key] )
        res[ "week_uv_list" ].append( week_uv_dict[key] )
        res[ "total_uv_list" ].append( total_uv_dict[key] )
        res[ "day_uv_percent" ].append( common_method.getListDivide( day_uv_dict[key] , day_uv_total , 100.0 ) )
        res[ "week_uv_percent" ].append( common_method.getListDivide( week_uv_dict[key] , week_uv_total , 100.0 ) )
        res[ "total_uv_percent" ].append( total_uv_dict[ key ] * 100.0 / total_uv_total if total_uv_total > 0 else 0 )
    return res

       
def query_multinavi_session_state_pv( st_time , ed_time , resid , version ):
    global conn
    global cur
    global sum_dict
    res = {}
    res[ "data_len" ]       = 0
    res[ "name_list" ]      = []
    res[ "hour_range" ]     = []
    res[ "day_range" ]      = []
    res[ "week_range" ]     = []
    
    res[ "hour_pv_list" ]     = []
    res[ "day_pv_list" ]      = []
    res[ "week_pv_list" ]     = []
    res[ "total_pv_list" ]    = []
    res[ "hour_pv_percent" ]     = []
    res[ "day_pv_percent" ]      = []
    res[ "week_pv_percent" ]     = []
    res[ "total_pv_percent" ]    = []
    
    cmd = "select state , time , query_num from " + sum_dict[ "resid_state_time_pv" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    if resid != "all" :
        cmd = cmd + " and resid=" + resid
    if version != "all" :
        cmd = cmd + " and version=" + version
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[1] , y[1] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[0][1] , qres[-1][1] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    name_dict       = {}
    hour_pv_dict , day_pv_dict , week_pv_dict , total_pv_dict = {} , {} , {} , {}
    hour_pv_total , day_pv_total , week_pv_total , total_pv_total = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:] , 0
    
    for qr in qres :
        key = qr[0]
        if key not in name_dict:
            name_dict[ key ]        = 1
            hour_pv_dict[ key ]     = hour_zero_list[0:]
            day_pv_dict[ key ]      = day_zero_list[0:]
            week_pv_dict[ key ]     = week_zero_list[0:] 
            total_pv_dict[ key ]    = 0
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[1] ) )
        query_num = string.atoi( qr[2] )
        hour_pv_dict[ key ][ hour_idx - hour_st ] += query_num
        day_pv_dict[ key ][ day_idx - day_st ]    += query_num
        week_pv_dict[ key ][ week_idx - week_st ] += query_num
        total_pv_dict[ key ]   += query_num
        hour_pv_total[ hour_idx - hour_st ] += query_num
        day_pv_total[ day_idx - day_st ]    += query_num
        week_pv_total[ week_idx - week_st ] += query_num
        total_pv_total += query_num
        
    for  key in name_dict :
        res[ "name_list" ].append( key )
        res[ "hour_pv_list" ].append( hour_pv_dict[key] )
        res[ "day_pv_list" ].append( day_pv_dict[key] )
        res[ "week_pv_list" ].append( week_pv_dict[key] )
        res[ "total_pv_list" ].append( total_pv_dict[key] )
        res[ "hour_pv_percent" ].append( common_method.getListDivide( hour_pv_dict[key] , hour_pv_total , 100.0 ) )
        res[ "day_pv_percent" ].append( common_method.getListDivide( day_pv_dict[key] , day_pv_total , 100.0 ) )
        res[ "week_pv_percent" ].append( common_method.getListDivide( week_pv_dict[key] , week_pv_total , 100.0 ) )
        res[ "total_pv_percent" ].append( total_pv_dict[ key ] * 100.0 / total_pv_total if total_pv_total > 0 else 0 )
    return res
    
    
def query_multinavi_session_time( st_time , ed_time , resid , version ):
    global conn
    global cur
    global sum_dict
    res = {}
    res[ "data_len" ]       = 0
    res[ "name_list" ]      = []
    res[ "day_range" ]      = []
    res[ "week_range" ]     = []
    res[ "day_time_list" ]      = []
    res[ "week_time_list" ]     = []
    res[ "total_time_list" ]    = []
    res[ "session_time_distribution_num" ]    = []
    res[ "session_time_distribution_percent" ]    = []
    
    cmd = "select resid , version , time , query_num, min_session_time, max_session_time,total_session_time, session_time_distribution from " + sum_dict[ "session_time_info" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    if resid != "all" :
        cmd = cmd + " and resid=" + resid
    if version != "all" :
        cmd = cmd + " and version=" + version
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[2] , y[2] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[0][2] , qres[-1][2] )
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    
    day_min_list , week_min_list , total_min_time = [ 99999999 for i in day_zero_list ] , [ 99999999 for i in week_zero_list ] , 99999999
    day_max_list , week_max_list , total_max_time = day_zero_list[0:] , week_zero_list[0:] , 0
    day_pv_list, day_total_time_list, week_pv_list, week_total_time_list = day_zero_list[0:], day_zero_list[0:],  week_zero_list[0:], week_zero_list[0:]
    total_pv_sum , total_time_sum = 0 , 0
    session_time_distribution = []
    
    for qr in qres :
        hour_idx, day_idx, week_idx = common_method.localHourToTimes( string.atoi( qr[2] ) )
        query_num, min_time, max_time, total_time = string.atoi( qr[3] ), string.atoi( qr[4] ), string.atoi( qr[5] ), string.atoi( qr[6] )
        time_distribution = [ string.atoi(i) for i in qr[7].split('|') ]
        day_min_list[day_idx - day_st]  = min_time if min_time < day_min_list[day_idx - day_st] else day_min_list[day_idx - day_st]
        day_max_list[day_idx - day_st]  = max_time if max_time > day_max_list[day_idx - day_st] else day_max_list[day_idx - day_st]
        week_min_list[week_idx - week_st]  = min_time if min_time < week_min_list[week_idx - week_st] else week_min_list[week_idx - week_st]
        week_max_list[week_idx - week_st]  = max_time if max_time < week_max_list[week_idx - week_st] else week_max_list[week_idx - week_st]
        total_min_time = min_time if min_time < total_min_time else total_min_time
        total_max_time = max_time if max_time > total_max_time else total_max_time
        day_pv_list[day_idx - day_st]          += query_num
        day_total_time_list[day_idx - day_st]  += total_time
        week_pv_list[week_idx - week_st]          += query_num
        week_total_time_list[week_idx - week_st]  += total_time
        total_pv_sum    += query_num
        total_time_sum  += total_time
        if len(session_time_distribution) <= 0 :
            session_time_distribution = time_distribution[0:]
        else:
            session_time_distribution = common_method.getListAdd(session_time_distribution, time_distribution)
    day_min_list = [0 if i==99999999 else i for i in day_min_list]
    week_min_list = [0 if i==99999999 else i for i in week_min_list]
    total_min_time = 0 if total_min_time==99999999 else total_min_time
    session_num = common_method.getListSum( session_time_distribution )
    session_num_list = [ session_num for i in session_time_distribution ]
    res["name_list"] = ["min_time", "max_time", "avg_time"]
    res[ "day_time_list" ] = [ day_min_list , day_max_list , common_method.getListDivide(day_total_time_list, day_pv_list, 1.0) ]
    res[ "week_time_list" ] = [ week_min_list , week_max_list , common_method.getListDivide(week_total_time_list, week_pv_list, 1.0) ]
    res[ "total_time_list" ] = [ total_min_time , total_max_time , total_time_sum / total_pv_sum if total_pv_sum>0 else 0]
    res[ "session_time_distribution_num" ] = session_time_distribution
    res[ "session_time_distribution_percent" ] = common_method.getListDivide( session_time_distribution , session_num_list , 100.0 )
    return res


    
def query_multinavi_session_state_combination( st_time , ed_time , resid , version ):
    global conn
    global cur
    global sum_dict
    res = {}
    res[ "data_len" ]       = 0
    res[ "name_list" ]      = []
    res[ "day_range" ]      = []
    res[ "week_range" ]     = []
    res[ "day_pv_list" ]      = []
    res[ "week_pv_list" ]     = []
    res[ "total_pv_list" ]    = []
    res[ "day_pv_percent" ]      = []
    res[ "week_pv_percent" ]     = []
    res[ "total_pv_percent" ]    = []
    cmd = "select resid , version , time , state_combination , query_num from " + sum_dict[ "state_combination_info" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    if resid != "all" :
        cmd = cmd + " and resid=" + resid
    if version != "all" :
        cmd = cmd + " and version=" + version
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[2] , y[2] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[0][2] , qres[-1][2] )
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    
    name_dict       = {}
    day_pv_dict , week_pv_dict , total_pv_dict = {} , {} , {}
    day_pv_total , week_pv_total , total_pv_total = day_zero_list[0:] , week_zero_list[0:] , 0
    
    for qr in qres :
        key = qr[3]
        if key not in name_dict:
            name_dict[ key ]        = 1
            day_pv_dict[ key ]      = day_zero_list[0:]
            week_pv_dict[ key ]     = week_zero_list[0:] 
            total_pv_dict[ key ]    = 0
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[2] ) )
        query_num = string.atoi( qr[4] )
        day_pv_dict[ key ][ day_idx - day_st ]    += query_num
        week_pv_dict[ key ][ week_idx - week_st ] += query_num
        total_pv_dict[ key ]   += query_num
        day_pv_total[ day_idx - day_st ]    += query_num
        week_pv_total[ week_idx - week_st ] += query_num
        total_pv_total += query_num
    for  key in name_dict :
        res[ "name_list" ].append( key )
        res[ "day_pv_list" ].append( day_pv_dict[key] )
        res[ "week_pv_list" ].append( week_pv_dict[key] )
        res[ "total_pv_list" ].append( total_pv_dict[key] )
        res[ "day_pv_percent" ].append( common_method.getListDivide( day_pv_dict[key] , day_pv_total , 100.0 ) )
        res[ "week_pv_percent" ].append( common_method.getListDivide( week_pv_dict[key] , week_pv_total , 100.0 ) )
        res[ "total_pv_percent" ].append( total_pv_dict[ key ] * 100.0 / total_pv_total if total_pv_total > 0 else 0 )
    return res


       
       
    
    
def query_special_route_type_pv( st_time , ed_time ):
    global conn
    global cur
    global sum_dict

    res = {}
    res[ "data_len" ]           = 0
    res[ "name_list" ]          = []
    
    res[ "hour_range" ]         = []
    res[ "hour_num_list" ]      = []
    res[ "hour_num_ratio" ]     = []
    res[ "hour_total_num" ]     = []
    
    res[ "day_range" ]         = []
    res[ "day_num_list" ]      = []
    res[ "day_num_ratio" ]     = []
    res[ "day_total_num" ]     = []
    res[ "day_week_num_list" ]  = []
    res[ "day_week_num_ratio" ] = []
    res[ "day_week_total_num" ] = []
    
    res[ "week_range" ]         = []
    res[ "week_num_list" ]      = []
    res[ "week_num_ratio" ]     = []
    res[ "week_total_num" ]     = []
    res[ "week_day_num_list" ]  = []
    res[ "week_day_num_ratio" ] = []
    res[ "week_day_total_num" ] = []
    res[ "all_num_list" ]       = []

    cmd = "select special_route_type , time , query_num  from " + sum_dict[ "special_route_type_info" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[1] , y[1] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    #print qres
        
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
        
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 1 ] , qres[ -1 ][ 1 ] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    day_to_week_idx , week_day_count = day_zero_list[0:] , week_zero_list[0:]
    for i in range( len( day_to_week_idx ) ):
        day_to_week_idx[i] = common_method.UTChourToWeekIdx( (i+day_st)*24 ) - week_st
        week_day_count[ day_to_week_idx[i] ] += 1
        
    name_dict , hour_num_dict , day_num_dict , week_num_dict , all_num_dict = {} , {} , {} , {} ,{}
    hour_num_all , day_num_all , week_num_all = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    hour_total_num , day_total_num , week_total_num = hour_zero_list[0:] , day_zero_list[0:] , week_zero_list[0:]
    
    for qr in qres :
        key = qr[0]
        if key == "-10" :
            continue;
        if key not in name_dict:
            name_dict[ key ] = 1
            hour_num_dict[ key ]    = hour_zero_list[0:]
            day_num_dict[ key ]     = day_zero_list[0:]
            week_num_dict[ key ]    = week_zero_list[0:]
            all_num_dict[ key ]     = 0
        query_num = string.atoi( qr[2] )
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[1] ) )
        hour_num_dict[ key ][ hour_idx - hour_st ]  += query_num
        day_num_dict[ key ][ day_idx - day_st ]     += query_num
        week_num_dict[ key ][ week_idx - week_st ]  += query_num
        hour_num_all[ hour_idx - hour_st ]  += query_num
        day_num_all[ day_idx - day_st ]     += query_num
        week_num_all[ week_idx - week_st ]  += query_num
        hour_total_num[ hour_idx - hour_st ]  += query_num
        day_total_num[ day_idx - day_st ]     += query_num
        week_total_num[ week_idx - week_st ]  += query_num
        all_num_dict[ key ] += query_num
    for key  in name_dict:
        res[ "name_list" ].append( key )
        res[ "hour_num_list" ].append( hour_num_dict[ key ] )
        res[ "hour_num_ratio" ].append( common_method.getListDivide( hour_num_dict[ key ] , hour_num_all , 100.0 ) )
        res[ "day_num_list" ].append( day_num_dict[ key ] )
        res[ "day_num_ratio" ].append( common_method.getListDivide( day_num_dict[ key ] , day_num_all , 100.0 ) )
        res[ "week_num_list" ].append( week_num_dict[ key ] )
        res[ "week_num_ratio" ].append( common_method.getListDivide( week_num_dict[ key ] , week_num_all , 100.0 ) )
        res[ "all_num_list" ].append( all_num_dict[ key ] )
        res[ "day_week_num_list" ].append( common_method.getDayWeekAvg( res[ "day_num_list" ][-1] , day_to_week_idx , week_day_count ) )
        res[ "day_week_num_ratio" ].append( common_method.getDayWeekAvg( res[ "day_num_ratio" ][-1] , day_to_week_idx , week_day_count ) )
        res[ "week_day_num_list" ].append( common_method.getWeekDayAvg( res[ "day_num_list" ][-1] , day_to_week_idx , week_day_count ) )
        res[ "week_day_num_ratio" ].append( common_method.getWeekDayAvg( res[ "day_num_ratio" ][-1] , day_to_week_idx , week_day_count ) )
    res["hour_total_num"] = hour_total_num
    res["day_total_num"] = day_total_num
    res["week_total_num"] = week_total_num
    res[ "day_week_total_num" ] = common_method.getDayWeekAvg( day_total_num , day_to_week_idx , week_day_count )
    res[ "week_day_total_num" ] = common_method.getWeekDayAvg( day_total_num , day_to_week_idx , week_day_count )
    return res        
    





def query_navigation_preference_summary( st_time , ed_time ) :
    global conn
    global cur
    global sum_dict
    cmd = "select sy , prefer , mrs , time , qt , version , query_num , err_num , total_time ,"
    cmd = cmd + " total_size , size_vector , time_vector from " + sum_dict[ "sy_prefer_mrs_time" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time )
    cmd = cmd + " and qt=\"rc\" and version=4"
    
    res = {}
    res[ "data_len" ]       = 0
    res[ "hour_range" ]     = []
    res[ "day_range" ]      = []
    res[ "week_range" ]     = []
    
    res[ "spm_name" ]       = []
    res[ "spm_data" ]       = []
    res[ "size_vector" ]    = prefer_size_vec[0:];
    res[ "time_vector" ]    = prefer_time_vec[0:];
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[3] , y[3] ) )
    
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    #print qres
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
    
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 3 ] , qres[ -1 ][ 3 ] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    size_zero_list  = [ 0 for i in range( 1 + len( prefer_size_vec ) ) ]
    time_zero_list  = [ 0 for i in range( 1 + len( prefer_time_vec ) ) ]
    
    spm_name_dict       = {}
    hour_spm_pv_dict    = {}
    day_spm_pv_dict     = {}
    week_spm_pv_dict    = {}
    hour_spm_err_dict   = {}
    day_spm_err_dict    = {}
    week_spm_err_dict   = {}
    hour_spm_ttm_dict   = {}
    day_spm_ttm_dict    = {}
    week_spm_ttm_dict   = {}
    hour_spm_tsz_dict   = {}
    day_spm_tsz_dict    = {}
    week_spm_tsz_dict   = {}
    spm_size_vec_dict   = {}
    spm_time_vec_dict   = {}
    hour_spm_total_pv_list  = hour_zero_list[0:]
    hour_spm_total_err_list = hour_zero_list[0:]
    day_spm_total_pv_list   = day_zero_list[0:]
    day_spm_total_err_list  = day_zero_list[0:]
    week_spm_total_pv_list  = week_zero_list[0:]
    week_spm_total_err_list = week_zero_list[0:]
    
    
    spm_qv_name_dict       = {}
    hour_spm_qv_pv_dict    = {}
    day_spm_qv_pv_dict     = {}
    week_spm_qv_pv_dict    = {}
    hour_spm_qv_err_dict   = {}
    day_spm_qv_err_dict    = {}
    week_spm_qv_err_dict   = {}
    hour_spm_qv_ttm_dict   = {}
    day_spm_qv_ttm_dict    = {}
    week_spm_qv_ttm_dict   = {}
    hour_spm_qv_tsz_dict   = {}
    day_spm_qv_tsz_dict    = {}
    week_spm_qv_tsz_dict   = {}
    spm_qv_size_vec_dict   = {}
    spm_qv_time_vec_dict   = {}
    
    #sy , prefer , mrs , time , qt , version , query_num , err_num , total_time total_size , size_vector , time_vector 
    for qr in qres :
        key = qr[0] + "," + qr[1] + "," + qr[2]
        if key not in sy_prefer_mrs_type_dict:
            continue
        key = sy_prefer_mrs_type_dict[ key ]
        key_qv = key + "|" + qr[4] + "," + qr[5] 
        if key not in spm_name_dict:
            spm_name_dict[ key ] = 1
            hour_spm_pv_dict[ key ] = hour_zero_list[0:]
            hour_spm_err_dict[ key ]= hour_zero_list[0:]
            hour_spm_ttm_dict[ key ]= hour_zero_list[0:]
            hour_spm_tsz_dict[ key ]= hour_zero_list[0:]
            day_spm_pv_dict[ key ]  = day_zero_list[0:]
            day_spm_err_dict[ key ] = day_zero_list[0:]
            day_spm_ttm_dict[ key ] = day_zero_list[0:]
            day_spm_tsz_dict[ key ] = day_zero_list[0:]
            week_spm_pv_dict[ key ] = week_zero_list[0:]
            week_spm_err_dict[ key ]= week_zero_list[0:]
            week_spm_ttm_dict[ key ]= week_zero_list[0:]
            week_spm_tsz_dict[ key ]= week_zero_list[0:]
            spm_size_vec_dict[ key ]= size_zero_list[0:]
            spm_time_vec_dict[ key ]= time_zero_list[0:]
        if key_qv not in spm_qv_name_dict:
            spm_qv_name_dict[ key_qv ] = 1
            hour_spm_qv_pv_dict[ key_qv ] = hour_zero_list[0:]
            hour_spm_qv_err_dict[ key_qv ]= hour_zero_list[0:]
            hour_spm_qv_ttm_dict[ key_qv ]= hour_zero_list[0:]
            hour_spm_qv_tsz_dict[ key_qv ]= hour_zero_list[0:]
            day_spm_qv_pv_dict[ key_qv ]  = day_zero_list[0:]
            day_spm_qv_err_dict[ key_qv ] = day_zero_list[0:]
            day_spm_qv_ttm_dict[ key_qv ] = day_zero_list[0:]
            day_spm_qv_tsz_dict[ key_qv ] = day_zero_list[0:]
            week_spm_qv_pv_dict[ key_qv ] = week_zero_list[0:]
            week_spm_qv_err_dict[ key_qv ]= week_zero_list[0:]
            week_spm_qv_ttm_dict[ key_qv ]= week_zero_list[0:]
            week_spm_qv_tsz_dict[ key_qv ]= week_zero_list[0:]
            spm_qv_size_vec_dict[ key_qv ]= size_zero_list[0:]
            spm_qv_time_vec_dict[ key_qv ]= time_zero_list[0:]
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[3] ) )
        pv_num , err_num , total_time , total_size = string.atoi( qr[6] ) , string.atoi( qr[7] ) , string.atoi( qr[8] ) , string.atoi( qr[9] )
        size_vector_list = size_zero_list[0:] if qr[10] == "" else [ string.atoi( qrr ) for qrr in qr[10].split( '|' ) ]
        time_vector_list = time_zero_list[0:] if qr[11] == "" else [ string.atoi( qrr ) for qrr in qr[11].split( '|' ) ]
        hour_spm_pv_dict[ key ][ hour_idx - hour_st ]       += pv_num
        hour_spm_err_dict[ key ][ hour_idx - hour_st ]      += err_num
        hour_spm_ttm_dict[ key ][ hour_idx - hour_st ]      += total_time
        hour_spm_tsz_dict[ key ][ hour_idx - hour_st ]      += total_size
        day_spm_pv_dict[ key ][ day_idx - day_st ]          += pv_num
        day_spm_err_dict[ key ][ day_idx - day_st ]         += err_num
        day_spm_ttm_dict[ key ][ day_idx - day_st ]         += total_time
        day_spm_tsz_dict[ key ][ day_idx - day_st ]         += total_size
        week_spm_pv_dict[ key ][ week_idx - week_st ]       += pv_num
        week_spm_err_dict[ key ][ week_idx - week_st ]      += err_num
        week_spm_ttm_dict[ key ][ week_idx - week_st ]      += total_time
        week_spm_tsz_dict[ key ][ week_idx - week_st ]      += total_size
        hour_spm_total_pv_list[ hour_idx - hour_st ]        += pv_num
        hour_spm_total_err_list[ hour_idx - hour_st ]       += err_num
        day_spm_total_pv_list[ day_idx - day_st ]           += pv_num
        day_spm_total_err_list[ day_idx - day_st ]          += err_num
        week_spm_total_pv_list[ week_idx - week_st ]        += pv_num
        week_spm_total_err_list[ week_idx - week_st ]       += err_num
        spm_size_vec_dict[ key ] = common_method.getListAdd( spm_size_vec_dict[ key ] , size_vector_list )
        spm_time_vec_dict[ key ] = common_method.getListAdd( spm_time_vec_dict[ key ] , time_vector_list )
        hour_spm_qv_pv_dict[ key_qv ][ hour_idx - hour_st ]    += pv_num
        hour_spm_qv_err_dict[ key_qv ][ hour_idx - hour_st ]   += err_num
        hour_spm_qv_ttm_dict[ key_qv ][ hour_idx - hour_st ]   += total_time
        hour_spm_qv_tsz_dict[ key_qv ][ hour_idx - hour_st ]   += total_size
        day_spm_qv_pv_dict[ key_qv ][ day_idx - day_st ]       += pv_num
        day_spm_qv_err_dict[ key_qv ][ day_idx - day_st ]      += err_num
        day_spm_qv_ttm_dict[ key_qv ][ day_idx - day_st ]      += total_time
        day_spm_qv_tsz_dict[ key_qv ][ day_idx - day_st ]      += total_size
        week_spm_qv_pv_dict[ key_qv ][ week_idx - week_st ]    += pv_num
        week_spm_qv_err_dict[ key_qv ][ week_idx - week_st ]   += err_num
        week_spm_qv_ttm_dict[ key_qv ][ week_idx - week_st ]   += total_time
        week_spm_qv_tsz_dict[ key_qv ][ week_idx - week_st ]   += total_size
        spm_qv_size_vec_dict[ key_qv ] = common_method.getListAdd( spm_qv_size_vec_dict[ key_qv ] , size_vector_list )
        spm_qv_time_vec_dict[ key_qv ] = common_method.getListAdd( spm_qv_time_vec_dict[ key_qv ] , time_vector_list )
        
             
    for key in spm_name_dict :
        data_dict   = {}
        tmp_time_dis_num , tmp_size_dis_num =  common_method.getListSum( spm_time_vec_dict[ key ] ) , common_method.getListSum( spm_size_vec_dict[ key ] )
        data_dict[ "hour_pv_num" ]      = hour_spm_pv_dict[ key ]
        data_dict[ "hour_pv_percent" ]  = common_method.getListDivide( hour_spm_pv_dict[ key ] , hour_spm_total_pv_list , 100.0 )
        data_dict[ "hour_err_ratio" ]   = common_method.getListDivide( hour_spm_err_dict[ key ] , hour_spm_pv_dict[ key ] , 100.0 )
        data_dict[ "hour_err_percent" ] = common_method.getListDivide( hour_spm_err_dict[ key ] , hour_spm_total_err_list , 100.0 )
        data_dict[ "hour_avg_time" ]    = common_method.getListDivide( hour_spm_ttm_dict[ key ] , hour_spm_pv_dict[ key ] , 1.0 )
        data_dict[ "hour_avg_size" ]    = common_method.getListDivide( hour_spm_tsz_dict[ key ] , hour_spm_pv_dict[ key ] , 1.0 )
        data_dict[ "day_pv_num" ]      = day_spm_pv_dict[ key ]
        data_dict[ "day_pv_percent" ]  = common_method.getListDivide( day_spm_pv_dict[ key ] , day_spm_total_pv_list , 100.0 )
        data_dict[ "day_err_ratio" ]   = common_method.getListDivide( day_spm_err_dict[ key ] , day_spm_pv_dict[ key ] , 100.0 )
        data_dict[ "day_err_percent" ] = common_method.getListDivide( day_spm_err_dict[ key ] , day_spm_total_err_list , 100.0 )
        data_dict[ "day_avg_time" ]    = common_method.getListDivide( day_spm_ttm_dict[ key ] , day_spm_pv_dict[ key ] , 1.0 )
        data_dict[ "day_avg_size" ]    = common_method.getListDivide( day_spm_tsz_dict[ key ] , day_spm_pv_dict[ key ] , 1.0 )
        data_dict[ "week_pv_num" ]      = week_spm_pv_dict[ key ]
        data_dict[ "week_pv_percent" ]  = common_method.getListDivide( week_spm_pv_dict[ key ] , week_spm_total_pv_list , 100.0 )
        data_dict[ "week_err_ratio" ]   = common_method.getListDivide( week_spm_err_dict[ key ] , week_spm_pv_dict[ key ] , 100.0 )
        data_dict[ "week_err_percent" ] = common_method.getListDivide( week_spm_err_dict[ key ] , week_spm_total_err_list , 100.0 )
        data_dict[ "week_avg_time" ]    = common_method.getListDivide( week_spm_ttm_dict[ key ] , week_spm_pv_dict[ key ] , 1.0 )
        data_dict[ "week_avg_size" ]    = common_method.getListDivide( week_spm_tsz_dict[ key ] , week_spm_pv_dict[ key ] , 1.0 )
        data_dict[ "time_distribution" ]= common_method.getListDivide( spm_time_vec_dict[ key ] , [ tmp_time_dis_num for k in spm_time_vec_dict[ key ] ] , 100.0 )
        data_dict[ "size_distribution" ]= common_method.getListDivide( spm_size_vec_dict[ key ] , [ tmp_size_dis_num for k in spm_size_vec_dict[ key ] ] , 100.0 )
        data_dict[ "qt_version_data" ]  = []
        data_dict[ "qt_version_name" ]  = []
        res[ "spm_data" ].append( data_dict )
        res[ "spm_name" ].append( key )
        #kkkk_hour_kkkk = 1
    for key in spm_qv_name_dict:
        data_dict   = {}
        spm_name    = key.split( '|' )[0]
        qt_version  = key.split( '|' )[1]
        tmp_time_dis_num , tmp_size_dis_num =  common_method.getListSum( spm_qv_time_vec_dict[ key ] ) , common_method.getListSum( spm_qv_size_vec_dict[ key ] )
        data_dict[ "hour_pv_num" ]      = hour_spm_qv_pv_dict[ key ]
        data_dict[ "hour_pv_percent" ]  = common_method.getListDivide( hour_spm_qv_pv_dict[ key ] , hour_spm_pv_dict[ spm_name ] , 100.0 )
        data_dict[ "hour_err_ratio" ]   = common_method.getListDivide( hour_spm_qv_err_dict[ key ] , hour_spm_qv_pv_dict[ key ] , 100.0 )
        data_dict[ "hour_err_percent" ] = common_method.getListDivide( hour_spm_qv_err_dict[ key ] , hour_spm_err_dict[ spm_name ] , 100.0 )
        data_dict[ "hour_avg_time" ]    = common_method.getListDivide( hour_spm_qv_ttm_dict[ key ] , hour_spm_qv_pv_dict[ key ] , 1.0 )
        data_dict[ "hour_avg_size" ]    = common_method.getListDivide( hour_spm_qv_tsz_dict[ key ] , hour_spm_qv_pv_dict[ key ] , 1.0 )
        data_dict[ "day_pv_num" ]       = day_spm_qv_pv_dict[ key ]
        data_dict[ "day_pv_percent" ]   = common_method.getListDivide( day_spm_qv_pv_dict[ key ] , day_spm_pv_dict[ spm_name ] , 100.0 )
        data_dict[ "day_err_ratio" ]    = common_method.getListDivide( day_spm_qv_err_dict[ key ] , day_spm_qv_pv_dict[ key ] , 100.0 )
        data_dict[ "day_err_percent" ]  = common_method.getListDivide( day_spm_qv_err_dict[ key ] , day_spm_err_dict[ spm_name ] , 100.0 )
        data_dict[ "day_avg_time" ]     = common_method.getListDivide( day_spm_qv_ttm_dict[ key ] , day_spm_qv_pv_dict[ key ] , 1.0 )
        data_dict[ "day_avg_size" ]     = common_method.getListDivide( day_spm_qv_tsz_dict[ key ] , day_spm_qv_pv_dict[ key ] , 1.0 )
        data_dict[ "week_pv_num" ]      = week_spm_qv_pv_dict[ key ]
        data_dict[ "week_pv_percent" ]  = common_method.getListDivide( week_spm_qv_pv_dict[ key ] , week_spm_pv_dict[ spm_name ] , 100.0 )
        data_dict[ "week_err_ratio" ]   = common_method.getListDivide( week_spm_qv_err_dict[ key ] , week_spm_qv_pv_dict[ key ] , 100.0 )
        data_dict[ "week_err_percent" ] = common_method.getListDivide( week_spm_qv_err_dict[ key ] , week_spm_err_dict[ spm_name ] , 100.0 )
        data_dict[ "week_avg_time" ]    = common_method.getListDivide( week_spm_qv_ttm_dict[ key ] , week_spm_qv_pv_dict[ key ] , 1.0 )
        data_dict[ "week_avg_size" ]    = common_method.getListDivide( week_spm_qv_tsz_dict[ key ] , week_spm_qv_pv_dict[ key ] , 1.0 )
        data_dict[ "time_distribution" ]= common_method.getListDivide( spm_qv_time_vec_dict[ key ] , [ tmp_time_dis_num for k in spm_qv_time_vec_dict[ key ] ] , 100.0 )
        data_dict[ "size_distribution" ]= common_method.getListDivide( spm_qv_size_vec_dict[ key ] , [ tmp_size_dis_num for k in spm_qv_size_vec_dict[ key ] ] , 100.0 )
        for i in range( len( res[ "spm_name" ] ) ):
            if res[ "spm_name" ][i] == spm_name:
                res[ "spm_data" ][ i ][ "qt_version_name" ].append( qt_version )
                res[ "spm_data" ][ i ][ "qt_version_data" ].append( data_dict )
                break
    #print res
    return res
    

    
    



def query_route_time_ratio_summary( st_time , ed_time ):
    global conn
    global cur
    global sum_dict
    print st_time , ed_time
    res = {}
    res[ "data_len" ]           = 0
    res[ "day_range" ]          = []
    res[ "week_range" ]         = []
    res[ "ratio_dis" ]          = []
    res[ "day_avg_ratio" ]      = []
    res[ "week_avg_ratio" ]     = []
    res[ "all_avg_ratio" ]      = []
    cmd = "select time , tr_dis , tr_sum  from " + sum_dict[ "yaw_time_info_extern" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[0] , y[0] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
        
    if len( qres ) <=0 :
            return res
            
    res[ "data_len" ] = len( qres )
    
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 0 ] , string.atoi( qres[ -1 ][ 0 ] ) )
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    ratio_dis       = [ 0 for k in qres[0][1].split('|') ]
    
    day_ratio_sum   = day_zero_list[ 0 : ]
    day_dis_sum     = day_zero_list[ 0 : ]
    week_ratio_sum  = week_zero_list[ 0 : ]
    week_dis_sum    = week_zero_list[ 0 : ]
    all_ratio_sum   = 0
    all_dis_sum     = 0
    
    for qr in qres :
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[0] ) )
        ratio_dis_list = [ string.atoi(k) for k in qr[1].split( '|' ) ]
        ratio_sum_list = [ [ string.atoi( kk ) for kk in  k.split(',') ] for k in qr[2].split( '|' ) ]
        all_ratio_sum += ratio_sum_list[1][0]
        all_dis_sum   += ratio_sum_list[1][1]
        day_dis_sum[ day_idx - day_st ] += ratio_sum_list[1][1]
        week_dis_sum[ week_idx - week_st ] += ratio_sum_list[1][1]
        day_ratio_sum[ day_idx - day_st ] += ratio_sum_list[1][0]
        week_ratio_sum[ week_idx - week_st ] += ratio_sum_list[1][0]
        ratio_dis = common_method.getListAdd( ratio_dis , ratio_dis_list )
    res[ "ratio_dis" ]      = ratio_dis
    res[ "all_avg_ratio" ]  = all_ratio_sum  / ( all_dis_sum * 10000.0 );
    res[ "day_avg_ratio" ]  = common_method.getListDivide( day_ratio_sum , day_dis_sum , 0.0001 )
    res[ "week_avg_ratio" ] = common_method.getListDivide( week_ratio_sum , week_dis_sum , 0.0001 )
    return res












def query_multi_route_recall_info( st_time , ed_time ):
    global conn
    global cur
    global sum_dict
    
    #cgn: client_get_num
    #rsn: re_src_num
    MAX_CGN_NUMBER = 5
    MAX_RSN_NUMBER = 75
    
    
    res = {}
    res[ "data_len" ]           = 0
    res[ "hour_range" ]         = []
    res[ "day_range" ]          = []
    res[ "week_range" ]         = []
    
    res[ "prefer_name_list" ]   = []
    res[ "prefer_info_list" ]   = []
    res[ "city_name_list" ]     = []
    res[ "city_info_list" ]     = []
    cmd = "select city , prefer , time , in_vector , out_vector , in_out_vector  from " + sum_dict[ "multi_route_recall_info" ]
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time ) 
    cmd = cmd + " and ( prefer=16 or prefer=1 )"
    
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[2] , y[2] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    #print qres
        
    if len( qres ) <=0 :
        return res
            
    res[ "data_len" ] = len( qres )
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 2 ] , qres[ -1 ][ 2 ] )
    
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    cgn_zero_list   = [ 0 for i in range( MAX_CGN_NUMBER + 1 ) ] 
    rsn_zero_list   = [ 0 for i in range( MAX_RSN_NUMBER + 1 ) ]
    
    
    prefer_name_dict            = {}
    prefer_cgn_count_dict       = {}
    prefer_cgn_hour_cnt_dict    = {}
    prefer_cgn_day_cnt_dict     = {}
    prefer_cgn_week_cnt_dict    = {}
    prefer_rsn_count_dict       = {}
    
    city_prefer_name_dict           = {}
    city_prefer_cgn_count_dict      = {}
    city_prefer_cgn_hour_cnt_dict   = {}
    city_prefer_cgn_day_cnt_dict    = {}
    city_prefer_cgn_week_cnt_dict   = {}
    city_prefer_rsn_count_dict      = {}
    

    for qr in qres :
        prefer_key = qr[1]
        city_prefer_key = qr[0] + "|" + qr[1]
        rsn_num  = [ k.split( ':' ) for k in qr[3].split( '|' )[0:-1] ]
        rsn_num  = [ [ string.atoi( k[0] ) , string.atoi( k[1] ) ] for k in rsn_num ]
        cgn_num  = [ k.split( ':' ) for k in qr[4].split( '|' )[0:-1] ]
        cgn_num  = [ [ string.atoi( k[0] ) , string.atoi( k[1] ) ] for k in cgn_num ]
        if prefer_key not in prefer_name_dict:
            prefer_name_dict[ prefer_key ] = 1
            prefer_cgn_count_dict[ prefer_key ]       = cgn_zero_list[0:]
            prefer_cgn_hour_cnt_dict[ prefer_key ]    = [ hour_zero_list[0:] for i in range( MAX_CGN_NUMBER + 1 ) ]
            prefer_cgn_day_cnt_dict[ prefer_key ]     = [ day_zero_list[0:] for i in range( MAX_CGN_NUMBER + 1 ) ]
            prefer_cgn_week_cnt_dict[ prefer_key ]    = [ week_zero_list[0:] for i in range( MAX_CGN_NUMBER + 1 ) ]
            prefer_rsn_count_dict[ prefer_key ]       = rsn_zero_list[0:]
        if city_prefer_key not in city_prefer_name_dict:
            city_prefer_name_dict[ city_prefer_key ] = 1
            city_prefer_cgn_count_dict[ city_prefer_key ]       = cgn_zero_list[0:]
            city_prefer_cgn_hour_cnt_dict[ city_prefer_key ]    = [ hour_zero_list[0:] for i in range( MAX_CGN_NUMBER + 1 ) ]
            city_prefer_cgn_day_cnt_dict[ city_prefer_key ]     = [ day_zero_list[0:] for i in range( MAX_CGN_NUMBER + 1 ) ]
            city_prefer_cgn_week_cnt_dict[ city_prefer_key ]    = [ week_zero_list[0:] for i in range( MAX_CGN_NUMBER + 1 ) ]
            city_prefer_rsn_count_dict[ city_prefer_key ]       = rsn_zero_list[0:]
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[2] ) )
        for k in rsn_num:
            prefer_rsn_count_dict[ prefer_key ][ k[0] ] += k[1]
            city_prefer_rsn_count_dict[ city_prefer_key ][ k[0] ] += k[1]
        for k in cgn_num:
            prefer_cgn_count_dict[ prefer_key ][ k[0] ] += k[1]
            prefer_cgn_hour_cnt_dict[ prefer_key ][ k[0] ][ hour_idx - hour_st ] += k[1]
            prefer_cgn_day_cnt_dict[ prefer_key ][ k[0] ][ day_idx - day_st ]    += k[1]
            prefer_cgn_week_cnt_dict[ prefer_key ][ k[0] ][ week_idx - week_st ] += k[1]
            
            city_prefer_cgn_count_dict[ city_prefer_key ][ k[0] ] += k[1]
            city_prefer_cgn_hour_cnt_dict[ city_prefer_key ][ k[0] ][ hour_idx - hour_st ] += k[1]
            city_prefer_cgn_day_cnt_dict[ city_prefer_key ][ k[0] ][ day_idx - day_st ]    += k[1]
            city_prefer_cgn_week_cnt_dict[ city_prefer_key ][ k[0] ][ week_idx - week_st ] += k[1]
    
    
    for prefer_key in prefer_name_dict:
        res[ "prefer_name_list" ].append( prefer_key )
        print res[ "prefer_name_list" ]
        data_dict = {}
        data_dict[ "client_get_num_name" ]  = []
        data_dict[ "client_get_num_count" ] = []
        data_dict[ "client_get_num_hour_list" ]  = []
        data_dict[ "client_get_num_day_list" ]   = []
        data_dict[ "client_get_num_week_list" ]  = []
        data_dict[ "re_src_num_name" ]  = []
        data_dict[ "re_src_num_count" ] = []
        cgn_count    = prefer_cgn_count_dict[ prefer_key ]
        cgn_hour_cnt = prefer_cgn_hour_cnt_dict[ prefer_key ]
        cgn_day_cnt  = prefer_cgn_day_cnt_dict[ prefer_key ]
        cgn_week_cnt = prefer_cgn_week_cnt_dict[ prefer_key ]
        rsn_count    = prefer_rsn_count_dict[ prefer_key ]
        
        for i in range( len( cgn_count ) ):
            if cgn_count[i] == 0 :
                continue
            data_dict[ "client_get_num_name" ].append( i )
            data_dict[ "client_get_num_count" ].append( cgn_count[i] )
            data_dict[ "client_get_num_hour_list" ].append( cgn_hour_cnt[i] )
            data_dict[ "client_get_num_day_list" ].append( cgn_day_cnt[i] )
            data_dict[ "client_get_num_week_list" ].append( cgn_week_cnt[i] )
        for i in range( len( rsn_count ) ):
            if rsn_count[i] == 0 :
                continue
            data_dict[ "re_src_num_name" ].append( i )
            data_dict[ "re_src_num_count" ].append( rsn_count[i] )
        res[ "prefer_info_list" ].append( data_dict )
        
        
        
    res[ "city_name_list" ]     = []
    res[ "city_info_list" ]     = []
    city_dict , city_count = {} , 0
    for city_prefer_key in city_prefer_name_dict:
        [ city , prefer ]   = city_prefer_key.split( '|' )
        if city not in city_dict:
            res[ "city_name_list" ].append( city )
            res[ "city_info_list" ].append( { "prefer_name_list":[] , "prefer_info_list":[] } )
            city_dict[ city ] = city_count
            city_count += 1
        data_dict = {}
        data_dict[ "client_get_num_name" ]  = []
        data_dict[ "client_get_num_count" ] = []
        data_dict[ "client_get_num_hour_list" ]  = []
        data_dict[ "client_get_num_day_list" ]   = []
        data_dict[ "client_get_num_week_list" ]  = []
        data_dict[ "re_src_num_name" ]  = []
        data_dict[ "re_src_num_count" ] = []
        cgn_count    = city_prefer_cgn_count_dict[ city_prefer_key ]
        cgn_hour_cnt = city_prefer_cgn_hour_cnt_dict[ city_prefer_key ]
        cgn_day_cnt  = city_prefer_cgn_day_cnt_dict[ city_prefer_key ]
        cgn_week_cnt = city_prefer_cgn_week_cnt_dict[ city_prefer_key ]
        rsn_count    = city_prefer_rsn_count_dict[ city_prefer_key ]
        
        for i in range( len( cgn_count ) ):
            if cgn_count[i] == 0 :
                continue
            data_dict[ "client_get_num_name" ].append( i )
            data_dict[ "client_get_num_count" ].append( cgn_count[i] )
            data_dict[ "client_get_num_hour_list" ].append( cgn_hour_cnt[i] )
            data_dict[ "client_get_num_day_list" ].append( cgn_day_cnt[i] )
            data_dict[ "client_get_num_week_list" ].append( cgn_week_cnt[i] )
        for i in range( len( rsn_count ) ):
            if rsn_count[i] == 0 :
                continue
            data_dict[ "re_src_num_name" ].append( i )
            data_dict[ "re_src_num_count" ].append( rsn_count[i] )    
        city_idx = city_dict[ city ]
        res[ "city_info_list" ][ city_idx ][ "prefer_name_list" ].append( prefer )
        res[ "city_info_list" ][ city_idx ][ "prefer_info_list" ].append( data_dict )
    #print res
    return res
    
    


    
    
def query_naviure_type_flag( st_time , ed_time ) :
    global conn
    global cur
    global sum_dict
    cmd = "select type_flag , time , num from " + sum_dict[ "naviure_type_flag_info" ] 
    cmd = cmd + " where time >= " + str( st_time ) + " and time <= " + str( ed_time )
    
    res = {}
    res[ "data_len" ]       = 0
    res[ "hour_range" ]     = []
    res[ "day_range" ]      = []
    res[ "week_range" ]     = []
    res[ "name_list" ]      = []
    res[ "bad_name_list" ]  = []
    res[ "hour_num_list" ]  = []
    res[ "day_num_list" ]   = []
    res[ "week_num_list" ]  = []
    res[ "all_num_list" ]   = []
    res[ "hour_bad_num_list" ]  = []
    res[ "day_bad_num_list" ]   = []
    res[ "week_bad_num_list" ]  = []
    res[ "all_bad_num_list" ]   = []
    
    cur.execute( cmd )
    conn.commit( )
    qres = cur.fetchall()
    qres = [ qr for qr in qres ]
    qres.sort( lambda x,y:cmp( x[1] , y[1] ) )
    qres = [ [ str(qrr) for qrr in qr ] for qr in qres ]
    if len( qres ) <=0 :
        return res
    res[ "data_len" ] = len( qres )
    hour_st , hour_ed , day_st , day_ed , week_st , week_ed = common_method.localHourToTimeRanges( qres[ 0 ][ 1 ] , qres[ -1 ][ 1 ] )
    res[ "hour_range" ] = [ hour_st , hour_ed ]
    res[ "day_range" ]  = [ day_st * 24 , day_ed * 24 ]
    res[ "week_range" ] = [ common_method.weekIdxToHourUTC( week_st ) , common_method.weekIdxToHourUTC( week_ed ) ]
    
    
    hour_zero_list  = [ 0 for i in range( 1 + hour_ed - hour_st ) ]
    day_zero_list   = [ 0 for i in range( 1 + day_ed - day_st ) ]
    week_zero_list  = [ 0 for i in range( 1 + week_ed - week_st ) ]
    
    name_dict , bad_name_dict  = {} , {}
    hour_num_dict , day_num_dict , week_num_dict , all_num_dict = {} , {} , {} , {}
    hour_bad_num_dict , day_bad_num_dict , week_bad_num_dict , all_bad_num_dict = {} , {} , {} , {}
            
    for qr in qres :
        key = qr[0]
        if key == "" :
            continue;
        num = string.atoi( qr[2] )
        hour_idx , day_idx , week_idx = common_method.localHourToTimes( string.atoi( qr[1] ) )
        if key not in type_flag_type_dict:
            if key not in bad_name_dict:
                hour_bad_num_dict[ key ]    = hour_zero_list[ 0 : ]
                day_bad_num_dict[ key ]     = day_zero_list[ 0 : ]
                week_bad_num_dict[ key ]    = week_zero_list[ 0 : ]
                all_bad_num_dict[ key ]     = 0
                bad_name_dict[ key ]        = 1
            hour_bad_num_dict[ key ][ hour_idx - hour_st ] += num
            day_bad_num_dict[ key ][ day_idx - day_st ]    += num
            week_bad_num_dict[ key ][ week_idx - week_st ] += num
            all_bad_num_dict[ key ] += num
            key = "bad_all"
        if key not in name_dict:
            hour_num_dict[ key ]    = hour_zero_list[ 0 : ]
            day_num_dict[ key ]     = day_zero_list[ 0 : ]
            week_num_dict[ key ]    = week_zero_list[ 0 : ]
            all_num_dict[ key ]     = 0
            name_dict[ key ]        = 1
        hour_num_dict[ key ][ hour_idx - hour_st ] += num
        day_num_dict[ key ][ day_idx - day_st ]    += num
        week_num_dict[ key ][ week_idx - week_st ] += num
        all_num_dict[ key ] += num
        
    for key in name_dict:
        res[ "name_list" ].append( key )
        res[ "hour_num_list" ].append( hour_num_dict[ key ] )
        res[ "day_num_list" ].append( day_num_dict[ key ] )
        res[ "week_num_list" ].append( week_num_dict[ key ] )
        res[ "all_num_list" ].append( all_num_dict[ key ] )
    for key in bad_name_dict:
        res[ "bad_name_list" ].append( key )
        res[ "hour_bad_num_list" ].append( hour_bad_num_dict[ key ] )
        res[ "day_bad_num_list" ].append( day_bad_num_dict[ key ] )
        res[ "week_bad_num_list" ].append( week_bad_num_dict[ key ] )
        res[ "all_bad_num_list" ].append( all_bad_num_dict[ key ] )
    return res





#time stamp UTC
def divide_time_to_table( st_time , ed_time , table_type ):
    table_list = []
    for tc in tb_can :
        if table_type != tc[2] :
            continue
        table_st = string.atoi( tc[0] )
        table_et = table_st + 24 * 3600 - 1
        if tc[1] == "W" :
            table_et = table_st + 24 * 3600 * 7 - 1
        if tc[1] == "T" :
            table_et = table_st + 24 * 3600 * 3 - 1
        #print tc , table_st , table_et
        if table_et < st_time :
            continue
        if table_st > ed_time :
            continue
        table_list = table_list + [ tc ]
    table_list.sort( lambda x,y:cmp( string.atoi(x[0]), string.atoi(y[0]) ) )
    table_list = [ tl[3] for tl in table_list]
    return table_list





def query_error_query_type_limit( st_time , ed_time , table_name , error_num , line_st , line_num ) :
    global conn
    global cur
    global sum_dict
    cmd = "select * from " + table_name + " where tm >= " + str( st_time ) + " and tm <= " + str( ed_time )
    cmd = cmd + " and errno=" + str( error_num ) + " limit " + str( line_st ) + "," + str( line_num )
    try :
        cur.execute( cmd )
        conn.commit( )
        qres = cur.fetchall()
        qres = [ one for one in qres ]
        return qres
    except Exception as e:
        print e
    return []



def query_error_query_limit( st_time , ed_time , qt , version , table_name , line_st , line_num ) :
    global conn
    global cur
    global sum_dict
    cmd = "select * from " + table_name + " where tm >= " + str( st_time ) + " and tm <= " + str( ed_time ) 
    if qt != "" :
        cmd = cmd + " and qt=\"" + qt + "\""
        if version != "" :
            cmd = cmd + " and version=" + version 
    
    cmd = cmd + " limit " + str( line_st ) + "," + str( line_num )
    try :
        cur.execute( cmd )
        conn.commit( )
        qres = cur.fetchall()
        qres = [ one for one in qres ]
        return qres
    except Exception as e:
        print e
    return []





#query error information
def query_all_error_query_limit( st_time , ed_time , qt , version , line_st , line_num ) :
    tb_list = divide_time_to_table( st_time , ed_time , "E" )
    if len( tb_list ) > 0 :
        return query_error_query_limit( st_time , ed_time , qt , version , tb_list[0] , line_st , line_num )
    return []



def query_day_error_query_limit( st_time , qt , version , line_st , line_num ) :
    ed_time = st_time + 24*3600 - 1
    return query_all_error_query_limit( st_time , ed_time , qt , version , line_st , line_num )



def query_cuid_query_no_limit( cuid , st_time , ed_time , table_name ) :
    global conn
    global cur
    global sum_dict
    cmd = "select * from " + table_name + " where cuid=\"" + str( cuid ) + "\""
    cmd = cmd + " and tm >= " + str( st_time ) + " and tm <= " + str( ed_time )
    print cmd
    try :
        cur.execute( cmd )
        conn.commit( )
        qres = cur.fetchall()
        qres = [ one for one in qres ]
        return qres
    except Exception as e:
        print e
    return []


    
def query_imei_query_no_limit( imei , st_time , ed_time , table_name ) :
    global conn
    global cur
    global sum_dict
    cmd = "select * from " + table_name + " where imei=" + str( imei ) 
    cmd = cmd + " and tm >= " + str( st_time ) + " and tm <= " + str( ed_time )
    try :
        cur.execute( cmd )
        conn.commit( )
        qres = cur.fetchall()
        qres = [ one for one in qres ]
        return qres
    except Exception as e:
        print e
    return []
    
    

    

#query cuid information
def query_all_cuid_query_no_limit( cuid , st_time , ed_time ):
    tb_list = divide_time_to_table( st_time , ed_time , "N" )
    print tb_list
    query_res = []
    for tb in tb_list :
        query_res = query_res + query_cuid_query_no_limit( cuid , st_time , ed_time , tb )
    return query_res

    

    
def query_all_imei_query_no_limit( imei , st_time , ed_time ):
    tb_list = divide_time_to_table( st_time , ed_time , "N" )
    query_res = []
    for tb in tb_list :
        query_res = query_res + query_imei_query_no_limit( imei , st_time , ed_time , tb )
    return query_res
    

    
#query cuid information
def query_one_cuid_info_query( st_time , ed_time ):
    global conn
    global cur
    global sum_dict
    print "hello world"
    tb_list = divide_time_to_table( st_time , ed_time , "N" )
    res = ""
    for tb in tb_list :
        cmd = "select cuid from " + tb + " where tm>=" + str( st_time ) + " and tm<=" + str( ed_time ) + " limit 1 "
        cur.execute( cmd )
        conn.commit( )
        qres = cur.fetchall()
        qres = [ one for one in qres ]
        if len( qres ) > 0 :
            res = qres[0][0]
            break;
    return res    
    
    
    
    
    

def timer_update( interval ) :
    while True :
        refresh()
        time.sleep( interval );


def timer_update_connect( interval ) :
    while True :
        reconnect()
        time.sleep( interval );

thread.start_new_thread( timer_update , ( 1800 , ) )
thread.start_new_thread( timer_update_connect , ( 10800 , ) )

'''
if "__name__==__main__":
    for tc in tb_can :
        print tc
    tlst = divide_time_to_table( 1399132777 , 1400342400 , "E" )
    print tlst
    #print query_cuid_query_limit( "1234|12451" , 1399132777 , 1400342400 , "2014_05_18_D" , random.randint(10,100) , 3 )
    #print query_day_error_query_limit( 1400342400 , 1000 , 30 )
    for line in query_day_cuid_query_no_limit( "D63F35F15D70369D988786E3C34F1FDA|210381410088868" , 1400342400 ):
        print line
    cur.close()
    conn.close()

'''
