#coding:gbk
import web,os
import string
import time
import util
import sys
import common_method
import datetime
import mysql_search
import json
import send_mcpack

render = web.template.render("./templates/")


urls=(
    '/','index',
    '/top_all','top_all',
    '/top_all_get_data','top_all_get_data',
    '/kes_stat','kes_stat',
    '/kes_stat_get_data','kes_stat_get_data',
    '/cuid_log','cuid_log',
    '/cuid_log_get_data','cuid_log_get_data',
    '/error_log','error_log',
    '/error_log_get_data','error_log_get_data',
    
    '/port_pv','port_pv',
    '/port_pv_get_data','port_pv_get_data',
    '/port2version_pv_get_data','port2version_pv_get_data',
    '/port_error','port_error',
    '/port_error_get_data','port_error_get_data',
    '/port2version_error_get_data','port2version_error_get_data',
    '/port_illegal','port_illegal',
    '/port_illegal_get_data','port_illegal_get_data',
    '/port2version_illegal_get_data','port2version_illegal_get_data',
    '/port_data','port_data',
    '/port_data_get_data','port_data_get_data',
    '/port2version_data_get_data','port2version_data_get_data',
    '/port_performance','port_performance',
    '/port_performance_get_data','port_performance_get_data',
    '/port2version_performance_get_data','port2version_performance_get_data',
    '/yawpos_summary','yawpos_summary',
    '/yawpos_summary_get_data','yawpos_summary_get_data',
    '/yawpos_summary2version_get_data','yawpos_summary2version_get_data',
    
    '/version_pv','version_pv',
    '/version_pv_get_data','version_pv_get_data',
    '/version2port_pv_get_data','version2port_pv_get_data',
    '/version_error','version_error',
    '/version_error_get_data','version_error_get_data',
    '/version2port_error_get_data','version2port_error_get_data',
    '/version_illegal','version_illegal',
    '/version_illegal_get_data','version_illegal_get_data',
    '/version2port_illegal_get_data','version2port_illegal_get_data',
    '/version_data','version_data',
    '/version_data_get_data','version_data_get_data',
    '/version2port_data_get_data','version2port_data_get_data',
    '/version_performance','version_performance',
    '/version_performance_get_data','version_performance_get_data',
    '/version_performance2module_port_get_data','version_performance2module_port_get_data',
    '/module_performance','module_performance',
    '/module_performance_get_data','module_performance_get_data',
    
    '/naviure_type_flag' , 'naviure_type_flag',
    '/naviure_type_flag_get_data','naviure_type_flag_get_data',
    
    '/multinavi_session_query','multinavi_session_query',
    '/multinavi_session_query_get_data','multinavi_session_query_get_data',
    '/special_route_type_pv','special_route_type_pv',
    '/special_route_type_pv_get_data','special_route_type_pv_get_data',
    '/city_query','city_query',
    '/city_query_get_data','city_query_get_data',
    '/city_hf','city_hf',
    '/get_bd09ll_points' , 'get_bd09ll_points',
    '/multi_route_recall' , 'multi_route_recall',
    '/multi_route_recall_get_data' , 'multi_route_recall_get_data',
    '/navigation_preference' , 'navigation_preference',
    '/navigation_preference_get_data','navigation_preference_get_data',
    '/route_time_ratio' , 'route_time_ratio',
    '/route_time_ratio_get_data','route_time_ratio_get_data'
)

app=web.application(urls,globals())

global_info = [ "cq01-qa-ssd1-qa4.cq01.baidu.com", "8899" ]


class index:
    def GET(self):
        return render.index( global_info)
    def POST(self):
        return render.index( global_info)

        
        
class top_all:
    def GET(self):
        return render.version_top_all( global_info )
    def POST(self):
        return render.version_top_all( global_info )
        

class top_all_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        date_from   = common_method.getDayAgoStr( -7 )
        date_to     = common_method.getDayAgoStr( -1 )
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) + 23
        city_num    = mysql_search.query_top_page_city_query_num()
        port_info   = mysql_search.query_top_page_port_time_data_err_pv( hour_from , hour_to )
        dict        = {}
        dict[ "city_num" ]  = city_num
        dict[ "port_info" ] = port_info
        #print dict
        return json.dumps( dict )
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        return web.seeother( '/top_all_get_data')
    
class kes_stat:
    def GET(self):
        return render.kes_stat_render( global_info )
    def POST(self):
        return render.kes_stat_render( global_info )

class kes_stat_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        dict        = mysql_search.query_kes_stat_info()
        return json.dumps( dict )
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        return web.seeother( '/kes_stat_get_data')
    
    
class cuid_log:
    def GET( selft ):
        return render.log_cuid_search( global_info )
    def POST( selft ):
        return render.log_cuid_search( global_info )

    
class cuid_log_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        cuid        = input_data.get('cuid')
        imei        = input_data.get('imei')
        
        if date_from == "" or date_from == None  or date_to == "" or date_to == None :
            date_from   = common_method.getDayAgoStr( -1 )
            date_to     = common_method.getDayAgoStr( -1 )
        
        time_from   = common_method.dateToTime( date_from )
        time_to     = common_method.dateToTime( date_to )
        time_to     = time_to + 3600 * 24 - 1
        
        query_res   = []
        
        if cuid == None :
            cuid = ""
        if imei == None :
            imei = ""
        
        if cuid == "" and imei == "" :
            cuid = mysql_search.query_one_cuid_info_query( time_from , time_to ) 
            #"0000027472DF1B5D1B4E72954F0CE495|282543050041753"
        
        print cuid , imei , date_from , date_to
        if cuid == "" :
            query_res   = mysql_search.query_all_imei_query_no_limit( imei , time_from , time_to )
        else :
            query_res   = mysql_search.query_all_cuid_query_no_limit( cuid , time_from , time_to )
            
        query_res   = [ [ str(qrr) for qrr in qr ] for qr in query_res ]
        
        for i in range( len( query_res ) ) :
            query_res[ i ][ 2 ] = time.strftime( "%Y-%m-%d\n%H:%M:%S", time.localtime( string.atol( query_res[ i ][ 2 ] ) ) )
        
        dict = {}
        dict['cuid_cur']    = cuid
        dict['imei_cur']    = imei
        dict['date_from']   = date_from
        dict['date_to']     = date_to
        dict['query_res']   = query_res
        return json.dumps( dict )
    def POST(selft):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        cuid        = input_data.get('cuid')
        imei        = input_data.get('imei')
        if date_from == None or date_to == None:
            date_from = ""
            date_to = ""
        if cuid == None :
            cuid = ""
        if imei == None :
            imeir = ""
        return web.seeother( '/cuid_log_get_data' 
                        + '?date_from=' + date_from 
                        + '&date_to=' + date_to 
                        + '&cuid=' + cuid  
                        + '&imei=' + imei );

        
class error_log:
    def GET( self ):
        return render.log_error_search( global_info )
    def POST(self):
        return render.log_error_search( global_info )
        
        
        
class error_log_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_data   = input_data.get('date')
        line_num    = input_data.get('line_num')
        qt          = input_data.get('qt')
        version     = input_data.get('version')
        
        if line_num == "" or line_num == None :
            line_num = 1
        if date_data == "" or date_data == None :
            date_data   = common_method.getLastDayStr()
            
        if qt == "" or qt == None :
            qt = ""
        elif version == "" or version == None :
            version = ""
            
        query_res   = []
        time_stamp  = common_method.dateToTime( date_data )
        
        query_res   = mysql_search.query_day_error_query_limit( time_stamp , qt , version , line_num , 20 )
        query_res   = [ [ str(qrr) for qrr in qr ] for qr in query_res ]
        for i in range( len( query_res ) ) :
            query_res[ i ][ 0 ] = time.strftime( "%Y-%m-%d\n%H:%M:%S", time.localtime( string.atol( query_res[ i ][ 0 ] ) ) )
        #print query_res
        
        #, date_cur , qt_cur , version_cur , line_num , query_res , 
        dict = {}
        dict['date_cur']    = date_data
        dict['qt_cur']      = qt
        dict['version_cur'] = version
        dict['line_num']    = line_num
        dict['query_res']   = query_res
        #print dict;
        return json.dumps( dict )
        
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_data   = input_data.get('date')
        line_num    = input_data.get('line_num')
        qt          = input_data.get('qt')
        version     = input_data.get('version')
        if line_num == "" or line_num == None :
            line_num = "1"
        if date_data == "" or date_data == None :
            date_data   = common_method.getLastDayStr()
        if qt == "" or qt == None :
            qt = ""
        elif version == "" or version == None :
            version = ""
        return web.seeother( '/error_log_get_data' + '?date=' + date_data + '&line_num=' + line_num + '&qt=' + qt  + '&version=' + version );
        

class get_bd09ll_points:        
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        input_str   = input_data.get('input_str')
        print input_str
        sn,en,routes = send_mcpack.get_routes_from_str( input_str )
        request_dict = {'routes' : routes , 'sn' : sn , 'en' : en } 
        return json.dumps( request_dict )
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        input_str   = input_data.get('input_str')
        print input_str
        return web.seeother( '/get_bd09ll_points?input_str=' + input_str );       



class port_pv:
    def GET(self):
        return render.port_pv_render( global_info )
    def POST(self):
        return render.port_pv_render( global_info )

class port_pv_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        dict    = mysql_search.query_port_pv_query( hour_from , hour_to )
        
        dict[ "date_from" ] = date_from
        dict[ "date_to" ]   = date_to
        return json.dumps( dict )
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        return web.seeother( '/port_pv_get_data' + '?date_from=' + date_from + '&date_to=' + date_to )
        
class port2version_pv_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        port_name   =   input_data.get('port_name')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if port_name == None or port_name == "" :
            port_name = "baidu_map"
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        dict        = mysql_search.query_port2version_pv_query( hour_from , hour_to , port_name )
        json_data   = json.dumps( dict )
        return json_data
        
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  =   web.input()
        date_from   =   input_data.get('date_from')
        date_to     =   input_data.get('date_to')
        port_name   =   input_data.get('port_name')
        return web.seeother( '/port2version_pv_get_data' + '?date_from=' + date_from + '&date_to=' + date_to + '&port_name=' + port_name )     

class port_error:
    def GET(self):
        return render.port_error_render( global_info )
    def POST(self):
        return render.port_error_render( global_info )

class port_error_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        print hour_from , hour_to
        dict    = mysql_search.query_port_error_query( hour_from , hour_to )
        
        dict[ "date_from" ] = date_from
        dict[ "date_to" ]   = date_to
        return json.dumps( dict )
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        return web.seeother( '/port_error_get_data' + '?date_from=' + date_from + '&date_to=' + date_to )
        
class port2version_error_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        port_name   =   input_data.get('port_name')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if port_name == None or port_name == "" :
            port_name = "baidu_map"
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        dict        = mysql_search.query_port2version_error_query( hour_from , hour_to , port_name )
        json_data   = json.dumps( dict )
        return json_data
        
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  =   web.input()
        date_from   =   input_data.get('date_from')
        date_to     =   input_data.get('date_to')
        port_name   =   input_data.get('port_name')
        return web.seeother( '/port2version_error_get_data' + '?date_from=' + date_from + '&date_to=' + date_to + '&port_name=' + port_name ) 

class port_illegal:
    def GET(self):
        return render.port_illegal_render( global_info )
    def POST(self):
        return render.port_illegal_render( global_info )

class port_illegal_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        dict    = mysql_search.query_port_illegal_query( hour_from , hour_to )
        dict[ "date_from" ] = date_from
        dict[ "date_to" ]   = date_to
        return json.dumps( dict )
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        return web.seeother( '/port_illegal_get_data' + '?date_from=' + date_from + '&date_to=' + date_to )
        
class port2version_illegal_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        port_name   =   input_data.get('port_name')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if port_name == None or port_name == "" :
            port_name = "baidu_map"
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        dict        = mysql_search.query_port2version_illegal_query( hour_from , hour_to , port_name )
        json_data   = json.dumps( dict )
        return json_data
        
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  =   web.input()
        date_from   =   input_data.get('date_from')
        date_to     =   input_data.get('date_to')
        port_name   =   input_data.get('port_name')
        return web.seeother( '/port2version_illegal_get_data' + '?date_from=' + date_from + '&date_to=' + date_to + '&port_name=' + port_name ) 
        

        
class port_data:
    def GET(self):
        return render.port_data_render( global_info )
    def POST(self):
        return render.port_data_render( global_info )

class port_data_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        dict    = mysql_search.query_port_data_query( hour_from , hour_to )
        dict[ "date_from" ] = date_from
        dict[ "date_to" ]   = date_to
        return json.dumps( dict )
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        return web.seeother( '/port_data_get_data' + '?date_from=' + date_from + '&date_to=' + date_to )
        
class port2version_data_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        port_name   =   input_data.get('port_name')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if port_name == None or port_name == "" :
            port_name = "baidu_map"
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        dict        = mysql_search.query_port2version_data_query( hour_from , hour_to , port_name )
        json_data   = json.dumps( dict )
        return json_data
        
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  =   web.input()
        date_from   =   input_data.get('date_from')
        date_to     =   input_data.get('date_to')
        port_name   =   input_data.get('port_name')
        return web.seeother( '/port2version_data_get_data' + '?date_from=' + date_from + '&date_to=' + date_to + '&port_name=' + port_name )        
        
        
        
         
class port_performance:
    def GET(self):
        return render.port_performance_render( global_info )
    def POST(self):
        return render.port_performance_render( global_info )

class port_performance_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        dict    = mysql_search.query_port_performance_query( hour_from , hour_to )
        dict[ "date_from" ] = date_from
        dict[ "date_to" ]   = date_to
        return json.dumps( dict )
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        return web.seeother( '/port_performance_get_data' + '?date_from=' + date_from + '&date_to=' + date_to )
        
class port2version_performance_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        port_name   =   input_data.get('port_name')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if port_name == None or port_name == "" :
            port_name = "baidu_map"
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        dict        = mysql_search.query_port2version_performance_query( hour_from , hour_to , port_name )
        json_data   = json.dumps( dict )
        return json_data
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  =   web.input()
        date_from   =   input_data.get('date_from')
        date_to     =   input_data.get('date_to')
        port_name   =   input_data.get('port_name')
        return web.seeother( '/port2version_performance_get_data' + '?date_from=' + date_from + '&date_to=' + date_to + '&port_name=' + port_name )        
        
        
           

class yawpos_summary:
    def GET(selft):
        return render.port_yaw_render( global_info )
    def POST(selft):
        return render.port_yaw_render( global_info )
        
class yawpos_summary_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        dict = {}
        yaw_num_dict = mysql_search.query_yawpos_summary( hour_from , hour_to )
        yaw_num_dict[ "date_from" ] = date_from
        yaw_num_dict[ "date_to" ]   = date_to
        
        type_dict   = mysql_search.query_yawpos_type_info_summary( hour_from , hour_to )
        type_dict[ "date_from" ] = date_from
        type_dict[ "date_to" ]   = date_to
        
        dict[ "yaw_dict" ] = yaw_num_dict
        dict[ "type_dict" ] = type_dict
        return json.dumps( dict )
        
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        return web.seeother( '/yawpos_summary_get_data' + '?date_from=' + date_from + '&date_to=' + date_to );

class yawpos_summary2version_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        yaw_num_port  =   input_data.get('yaw_num_port')
        yaw_type_port =   input_data.get('yaw_type_port')
        if date_from == "" or date_to == "" or date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if yaw_num_port == "" or yaw_num_port == None or yaw_type_port == "" or yaw_type_port == None:
            yaw_num_port = "baidu_map"
            yaw_type_port = "baidu-map"
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        
        yaw_num_dict = mysql_search.query_port2version_yawpos_summary( hour_from , hour_to , yaw_num_port )
        yaw_type_dict = mysql_search.query_port_yawpos_type_info_summary( hour_from , hour_to , yaw_type_port )
        yaw_num_dict[ "date_from" ] = date_from
        yaw_num_dict[ "date_to" ]   = date_to
        yaw_type_dict[ "date_from" ]= date_from
        yaw_type_dict[ "date_to" ]  = date_to
        dict = {}
        dict[ "yaw_dict" ] = yaw_num_dict
        dict[ "type_dict" ] = yaw_type_dict
        json_data   = json.dumps( dict )
        return json_data
        
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        yaw_num_port  =   input_data.get('yaw_num_port')
        yaw_type_port =   input_data.get('yaw_type_port')
        if date_from == "" or date_to == "" or date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if yaw_num_port == "" or yaw_num_port == None or yaw_type_port == "" or yaw_type_port == None:
            yaw_num_port = "baidu_map"
            yaw_type_port = "baidu-map"
        
        parameters = "date_from=" + date_from + "&date_to=" + date_to
        parameters = "&yaw_num_port=" + yaw_num_port + "&yaw_type_port=" + yaw_type_port
        return web.seeother( '/yawpos_summary2version_get_data?' + parameters )



   


class version_pv:
    def GET(self):
        return render.version_pv_render( global_info )
    def POST(self):
        return render.version_pv_render( global_info )       

        
class version_pv_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        print hour_from , hour_to
        hour_to     = hour_to + 23
        print hour_from , hour_to
        dict        = mysql_search.query_version_pv( hour_from , hour_to )
        dict[ "date_from" ] = date_from
        dict[ "date_to" ]   = date_to
        
        #print dict
        return json.dumps( dict );
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        return web.seeother( '/version_pv_get_data' + '?date_from=' + date_from + '&date_to=' + date_to );
        
class version2port_pv_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        qt          = input_data.get('qt')
        version     = input_data.get('version')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if qt == None or version == None or qt == "" or version == "":
            qt      = "rc"
            version = "4"
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        print hour_from , hour_to
        hour_to     = hour_to + 23
        print hour_from , hour_to
        dict        = mysql_search.query_version2port_pv( hour_from , hour_to , qt , version )
        dict[ "date_from" ] = date_from
        dict[ "date_to" ]   = date_to
        return json.dumps( dict );
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        qt          = input_data.get('qt')
        version     = input_data.get('version')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if qt == None or version == None or qt == "" or version == "":
            qt      = "rc"
            version = "4"
        
        parameters = 'date_from=' + date_from + '&date_to=' + date_to
        parameters = parameters + "&qt=" + qt + "&version=" + version
        return web.seeother( '/version2port_pv_get_data' + '?' + parameters );
        
        
        
        
class version_error:
    def GET(self):
        return render.version_error_render( global_info )
    def POST(self):
        return render.version_error_render( global_info )


        
        
class version_error_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        dict        = mysql_search.query_version_error( hour_from , hour_to )
        dict[ "date_from" ] = date_from
        dict[ "date_to" ]   = date_to
        return json.dumps( dict )
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        return web.seeother( '/version_error_get_data' + '?date_from=' + date_from + '&date_to=' + date_to );
        
class version2port_error_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        qt          = input_data.get('qt')
        version     = input_data.get('version')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if qt == None or version == None or qt == "" or version == "":
            qt      = "rc"
            version = "4"
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        dict        = mysql_search.query_version2port_error( hour_from , hour_to , qt , version )
        dict[ "date_from" ] = date_from
        dict[ "date_to" ]   = date_to
        return json.dumps( dict )
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        qt          = input_data.get('qt')
        version     = input_data.get('version')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if qt == None or version == None or qt == "" or version == "":
            qt      = "rc"
            version = "4"
        
        parameters = 'date_from=' + date_from + '&date_to=' + date_to
        parameters = parameters + "&qt=" + qt + "&version=" + version
        return web.seeother( '/version2port_error_get_data' + '?' + parameters );
        
  
  
        
class version_illegal:
    def GET(self):
        return render.version_illegal_render( global_info )
    def POST(self):
        return render.version_illegal_render( global_info )

        
class version_illegal_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        dict        = mysql_search.query_version_illegal( hour_from , hour_to )
        dict[ "date_from" ] = date_from
        dict[ "date_to" ]   = date_to
        return json.dumps( dict )
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        return web.seeother( '/version_illegal_get_data' + '?date_from=' + date_from + '&date_to=' + date_to );
        
class version2port_illegal_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        qt          = input_data.get('qt')
        version     = input_data.get('version')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if qt == None or version == None or qt == "" or version == "":
            qt      = "rc"
            version = "4"
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        dict        = mysql_search.query_version2port_illegal( hour_from , hour_to , qt , version )
        dict[ "date_from" ] = date_from
        dict[ "date_to" ]   = date_to
        return json.dumps( dict )
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        qt          = input_data.get('qt')
        version     = input_data.get('version')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if qt == None or version == None or qt == "" or version == "":
            qt      = "rc"
            version = "4"
        
        parameters = 'date_from=' + date_from + '&date_to=' + date_to
        parameters = parameters + "&qt=" + qt + "&version=" + version
        return web.seeother( '/version2port_illegal_get_data' + '?' + parameters );
        

  
        
        
        
class version_data:
    def GET(self):
        return render.version_data_render( global_info )
    def POST(self):
        return render.version_data_render( global_info )  
        
        
class version_data_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        dict        = mysql_search.query_version_data_size( hour_from , hour_to )
        dict[ "date_from" ]     = date_from
        dict[ "date_to" ]       = date_to
        return json.dumps( dict )
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        return web.seeother( '/version_data_get_data' + '?date_from=' + date_from + '&date_to=' + date_to );
        
class version2port_data_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        qt          = input_data.get('qt')
        version     = input_data.get('version')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if qt == None or version == None or qt == "" or version == "":
            qt      = "rc"
            version = "4"
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        dict        = mysql_search.query_version2port_data_size( hour_from , hour_to , qt , version )
        dict[ "date_from" ]     = date_from
        dict[ "date_to" ]       = date_to
        return json.dumps( dict )
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        qt          = input_data.get('qt')
        version     = input_data.get('version')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if qt == None or version == None or qt == "" or version == "":
            qt      = "rc"
            version = "4"
        
        parameters = 'date_from=' + date_from + '&date_to=' + date_to
        parameters = parameters + "&qt=" + qt + "&version=" + version
        return web.seeother( '/version2port_data_get_data' + '?' + parameters );     
        
class version_performance:
    def GET(selft):
        return render.version_performance_render( global_info )
    def POST(selft):
        return render.version_performance_render( global_info )
        
class version_performance_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        #///////////////////////////////////////////////////////////////////////
        dict        = mysql_search.query_qt_version_time_distribution( hour_from , hour_to )
        dict[ "date_from" ]     = date_from
        dict[ "date_to" ]       = date_to
        return json.dumps( dict )
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        return web.seeother( '/version_performance_get_data' + '?date_from=' + date_from + '&date_to=' + date_to );        
        
class version_performance2module_port_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        qt          = input_data.get('qt')
        version     = input_data.get('version')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if qt == None or version == None or qt == "" or version == "":
            qt      = "rc"
            version = "4"
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        dict        = {}
        module_dict = mysql_search.query_qt_version2module_time_distribution( hour_from , hour_to , qt , version )
        module_dict[ "date_from" ] = date_from
        module_dict[ "date_to" ]   = date_to
        port_dict = mysql_search.query_qt_version2port_time_distribution( hour_from , hour_to , qt , version )
        port_dict[ "date_from" ] = date_from
        port_dict[ "date_to" ]   = date_to
        dict["module_dict"] = module_dict
        dict["port_dict"] = port_dict
        return json.dumps( dict )
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        qt          = input_data.get('qt')
        version     = input_data.get('version')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if qt == None or version == None or qt == "" or version == "":
            qt      = "rc"
            version = "4"
        
        parameters = 'date_from=' + date_from + '&date_to=' + date_to
        parameters = parameters + "&qt=" + qt + "&version=" + version
        return web.seeother( '/version_performance2module_port_get_data' + '?' + parameters );            
        
        
class module_performance:
    def GET(selft):
        return render.module_performance_render( global_info )
    def POST(selft):
        return render.module_performance_render( global_info )
        
class module_performance_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        #///////////////////////////////////////////////////////////////////////
        dict        = mysql_search.query_qt_version2module_time_distribution( hour_from , hour_to , "" , "" )
        dict[ "date_from" ]     = date_from
        dict[ "date_to" ]       = date_to
        return json.dumps( dict )
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        return web.seeother( '/module_performance_get_data' + '?date_from=' + date_from + '&date_to=' + date_to );   
        
        
class naviure_type_flag:
    def GET(selft):
        return render.naviure_type_flag_render( global_info )
    def POST(selft):
        return render.naviure_type_flag_render( global_info )
        
class naviure_type_flag_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        dict        = mysql_search.query_naviure_type_flag( hour_from , hour_to )
        dict[ "date_from" ]     = date_from
        dict[ "date_to" ]       = date_to
        return json.dumps( dict )
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        return web.seeother( '/naviure_type_flag_get_data' + '?date_from=' + date_from + '&date_to=' + date_to );        
        
        



class multinavi_session_query:
    def GET(selft):
        return render.multinavi_session_render( global_info )
    def POST(selft):
        return render.multinavi_session_render( global_info )
       
class multinavi_session_query_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        resid       = input_data.get('resid')
        version     = input_data.get('version')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        if resid == None or resid == "":
            resid = "all"
        if version == None or version == "":
            version = "all"
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        uv_dict       = mysql_search.query_multinavi_session_uv( hour_from , hour_to );
        state_pv_dict = mysql_search.query_multinavi_session_state_pv( hour_from , hour_to , resid , version );
        state_combination_dict = mysql_search.query_multinavi_session_state_combination( hour_from , hour_to , resid , version );
        session_time_dict = mysql_search.query_multinavi_session_time( hour_from , hour_to , resid , version );
        dict = {}
        dict[ "date_from" ]     = date_from
        dict[ "date_to" ]       = date_to
        dict[ "uv_dict" ]       = uv_dict
        dict[ "state_pv_dict" ] = state_pv_dict
        dict[ "state_combination_dict" ] = state_combination_dict
        dict[ "session_time_dict" ] = session_time_dict
        #print dict
        return json.dumps( dict )
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        resid       = input_data.get('resid')
        version     = input_data.get('version')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        if resid == None or resid == "":
            resid = "all"
        if version == None or version == "":
            version = "all"
        parameters = 'date_from=' + date_from + '&date_to=' + date_to + '&resid=' + resid + '&version=' + version
        return web.seeother( '/multinavi_session_query_get_data?' + parameters );         
        


class special_route_type_pv:
    def GET(selft):
        return render.special_route_type_pv_render( global_info )
    def POST(selft):
        return render.special_route_type_pv_render( global_info )
       
class special_route_type_pv_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        dict = mysql_search.query_special_route_type_pv( hour_from , hour_to );
        dict[ "date_from" ]     = date_from
        dict[ "date_to" ]       = date_to
        return json.dumps( dict )
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        parameters = 'date_from=' + date_from + '&date_to=' + date_to
        return web.seeother( '/special_route_type_pv_get_data?' + parameters );    


        
class city_query:
    def GET(self):
        return render.city_render( global_info )
    def POST(self):
        return render.city_render( global_info )
        
class city_query_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        search_model    = input_data.get('search_model')
        port_name   = input_data.get('port_name')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        if search_model == None or search_model == "":
            search_model    = 'DAY'
        if port_name == None or port_name == "":
            port_name    = 'all_port'
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        
        if search_model == "WEEK" :
            week_id_start   = common_method.localHourToWeekIdx( hour_from )
            week_id_end = common_method.localHourToWeekIdx( hour_to )
            hour_from   = common_method.weekIdxToHourLocal( week_id_start )
            hour_to     = common_method.weekIdxToHourLocal( week_id_end ) + 7 * 24 - 1
            date_from   = common_method.hourToDate( hour_from )
            date_to     = common_method.hourToDate( hour_to )
        dict = {}
        if port_name == 'all_port':
            dict        = mysql_search.query_city_query_num( hour_from , hour_to , search_model )
        else:
            dict        = mysql_search.query_city_query_num_for_port( hour_from , hour_to , search_model , port_name )
        dict[ "date_from" ]     = date_from
        dict[ "date_to" ]   = date_to
        dict[ "search_model" ]  = search_model
        return json.dumps( dict )
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        search_model= input_data.get('search_model')
        port_name   = input_data.get('port_name')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        if search_model == None or search_model == "":
            search_model    = 'DAY'
        if port_name == None or port_name == "":
            port_name    = 'all_port'
        parameters = 'date_from=' + date_from + '&date_to=' + date_to 
        parameters = parameters + '&search_model=' + search_model + '&port_name=' + port_name
        return web.seeother( '/city_query_get_data?' + parameters );
        
class city_hf:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   =   input_data.get('date_from')
        date_to     =   input_data.get('date_to')
        city        =   input_data.get('city')
        
        if date_from == None or date_to == None :
            date_from   = common_method.getLastDayStr()
            date_to     = date_from
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        
        if date_from == "" or date_to == "" :
            date_from   = common_method.getLastDayStr()
            date_to     = date_from
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if city == None or city == "":
            city    = "beijing"
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        pp_list     = mysql_search.query_city_hf( hour_from , hour_to , city )
        dict = { 'city_hf' : pp_list }
        json_data   = json.dumps( dict )
        return json_data
        
        
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  =   web.input()
        date_from   =   input_data.get('date_from')
        date_to     =   input_data.get('date_to')
        city        =   input_data.get('city')
        return web.seeother( '/city_hf' + '?date_from=' + date_from + '&date_to=' + date_to + "&city=" + city );        
        
class multi_route_recall:
    def GET(self):
        return render.multi_route_recall_render( global_info )
    def POST(self):
        return render.multi_route_recall_render( global_info )
        
        
        
class multi_route_recall_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        dict        = mysql_search.query_multi_route_recall_info( hour_from , hour_to )
        dict[ "date_from" ] = date_from
        dict[ "date_to" ]   = date_to
        return json.dumps( dict )
        
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        return web.seeother( '/multi_route_recall_get_data' + '?date_from=' + date_from + '&date_to=' + date_to );        
        
        

        
class navigation_preference:
    def GET( selft ):
        return render.navigation_preference_render( global_info )
    def POST( selft ):
        return render.navigation_preference_render( global_info )        
        
        
class navigation_preference_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        dict        = mysql_search.query_navigation_preference_summary( hour_from , hour_to )
        dict[ "date_from" ] = date_from
        dict[ "date_to" ]   = date_to
        return json.dumps( dict )
        
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  =   web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        return web.seeother( '/navigation_preference_get_data' + '?date_from=' + date_from + '&date_to=' + date_to );        
        
        
        
        
        
        

        
        
        
class route_time_ratio:
    def GET( selft ):
        return render.route_time_ratio_render( global_info )
    def POST( selft ):
        return render.route_time_ratio_render( global_info )        
        
        
class route_time_ratio_get_data:
    def GET(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  = web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        hour_from   = common_method.dateToHour( date_from )
        hour_to     = common_method.dateToHour( date_to ) 
        hour_to     = hour_to + 23
        dict        = mysql_search.query_route_time_ratio_summary( hour_from , hour_to )
        dict[ "date_from" ] = date_from
        dict[ "date_to" ]   = date_to
        return json.dumps( dict )
        
    def POST(self):
        web.header('Access-Control-Allow-Origin',      '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        input_data  =   web.input()
        date_from   = input_data.get('date_from')
        date_to     = input_data.get('date_to')
        if date_from == "" or date_to == "" :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
            
        if date_from == None or date_to == None :
            date_from   = common_method.getDayAgoStr( -7 )
            date_to     = common_method.getDayAgoStr( -1 )
        return web.seeother( '/route_time_ratio_get_data' + '?date_from=' + date_from + '&date_to=' + date_to );

 
   
        
        
    
if __name__=="__main__":
    app.run()
