#coding:gbk
import os
import string
import time
import util
import sys
import time
import datetime


def special_to_random_char( s ):
    s = s.replace( "%23" , "#" );
    s = s.replace( "%26" , "&" );
    s = s.replace( "%2B" , "+" );
    s = s.replace( "%2F" , "\\" );
    s = s.replace( "%3D" , "=" );
    s = s.replace( "%3F" , "?" );
    s = s.replace( "%25" , "%" );
    return s


def getLastDayStr():
    d1  = datetime.datetime.now( )
    d2  = d1 + datetime.timedelta( days = -1 )
    res = d2.strftime( "%m/%d/%Y" )
    return res

    
def getDayAgoStr( delta ):
    d1  = datetime.datetime.now( )
    d2  = d1 + datetime.timedelta( days = delta )
    res = d2.strftime( "%m/%d/%Y" )
    return res
    

    

def get_pass_weeks( week_num ):
    if week_num <= 0 :
        week_num = 1
    sunday_num = 0
    day_range = []
    ds = datetime.datetime.now( )
    timedelta_list = range( -30 , 0 )
    timedelta_list.reverse()
    for tl in timedelta_list:
        dx  = ds + datetime.timedelta( days = tl )
        res = dx.strftime( "%u" )
        day_range.append( dx ) 
        if res == "7" :
            sunday_num = sunday_num + 1
        if sunday_num == week_num :
            break
    return day_range[-1].strftime( "%m/%d/%Y" ) , day_range[0].strftime( "%m/%d/%Y" )
        

    
def dateToHour( mydate ):
    time_stamp  = long( time.mktime( time.strptime( mydate , "%m/%d/%Y" ) ) )
    return time_stamp/3600
    
def dateToTime( mydate ):
    time_stamp  = long( time.mktime( time.strptime( mydate , "%m/%d/%Y" ) ) )
    return time_stamp


def hourToDate( myhour ):
    time_stamp  = myhour * 3600
    return time.strftime( "%m/%d/%Y", time.localtime( time_stamp ) )

    
    
def get_month_begin_hour( date_string ):
    date_info = date_string.split( '/' )
    date_string = date_info[0] + "/01/" + date_info[2]
    return dateToHour( date_string )


def get_month_end_hour( date_string ):
    date_info = date_string.split( '/' )
    last_day = [31,28,31,30,31,30,31,31,30,31,30,31]
    year = string.atoi( date_info[2] )
    if year%400==0 or ( year%4==0 and year%100!=0) :
        last_day[1]=29
    date_info[1] = str( last_day[ string.atoi( date_info[0] ) - 1 ] )
    date_string = date_info[0] + "/" + date_info[1] + "/" + date_info[2]
    return dateToHour( date_string ) + 23


def get_month_index_from_hour( myhour ):
    date_string = hourToDate( myhour )
    date_info = [ string.atoi( k ) for k in date_string.split( '/' ) ]
    return ( date_info[2] - 1970 ) * 12 + date_info[0] - 1


def get_month_hour_from_index( index ):
    year = 1970 + index / 12
    month = index % 12 + 1
    date_string = str( month ) + "/01/" + str( year )
    return dateToHour( date_string )

    
    
    
def UTChourToWeekIdx( hours ):
    return ( hours / 24 + 4 ) / 7
    
def localHourToWeekIdx( hours ):
    return ( ( hours + 8 ) / 24 + 4 ) / 7
    

def weekIdxToHourLocal( weeks ):
    return ( weeks * 7 - 4 ) * 24 - 8 

    
def weekIdxToHourUTC( weeks ):
    return weekIdxToHourLocal( weeks ) + 8
    

def localHourToTimes( hour ):
    return hour + 8 , ( hour + 8 ) / 24 , localHourToWeekIdx( hour )
    

def localHourToTimeRanges( hour_start , hour_end ):
    hour_start , hour_end   = string.atoi( str( hour_start ) ) + 8 , string.atoi( str( hour_end ) ) + 8
    day_start , day_end     = hour_start / 24 , hour_end / 24
    week_start , week_end   = UTChourToWeekIdx( hour_start ) , UTChourToWeekIdx( hour_end )
    return hour_start , hour_end , day_start , day_end , week_start , week_end
    
    
def getListDivide( lista , listb , mul ):
    list_res = []
    for i in range( len( lista ) ):
        if listb[i] == 0:
            list_res = list_res + [0]
        else:
            list_res = list_res + [ lista[i] * mul / listb[i] ]
            
    return list_res
    
    
    
def getListAdd( lista , listb ):
    lena = len( lista )
    lenb = len( listb )
    maxLen = lena if lena>lenb else lenb
    list_res = [0] * maxLen
    for i in range( lena ):
        list_res[i] += lista[i]
    for i in range( lenb ):
        list_res[i] += listb[i]
    return list_res
    
    
    
def getListSum( lt ):
    res = 0 
    for ltt in lt :
        res = res + ltt
    return res

    
def getWeekDayAvg( day_data_list , day_to_week_idx , week_day_count ):
    res = [ 0.0 for i in range( len( week_day_count ) ) ]
    for i in range( len( day_data_list ) ):
        res[ day_to_week_idx[i] ] += day_data_list[i]
    for i in range( len( res ) ):
        res[i] = res[i] / week_day_count[i] if week_day_count[i] > 0 else 0
    return res
    
def getDayWeekAvg( day_data_list , day_to_week_idx , week_day_count ):
    week_day_avg = getWeekDayAvg( day_data_list , day_to_week_idx , week_day_count )
    res = [ week_day_avg[ day_to_week_idx[i] ] for i in range( len( day_to_week_idx ) )]
    return res
        