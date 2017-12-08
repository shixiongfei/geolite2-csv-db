#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
    Copyright (c) 2017 Xiongfei Shi <jenson.shixf@gmail.com>
    
    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:
    
    The above copyright notice and this permission notice shall be included in
    all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
    THE SOFTWARE.

        http://shixf.com/
'''

import os
import sys
import math
import time
import select
import socket
import chardet
import pymysql
import ipaddress
import zipfile
import csv

try:
    # for py3
    from urllib.request import urlretrieve, HTTPError, URLError
except:
    # for py2
    from urllib import urlretrieve
    from urllib2 import HTTPError, URLError

try:
    # for py3
    from io import StringIO
except:
    # for py2
    from StringIO import StringIO


mysql_config = {
    'host' : '127.0.0.1',
    'port' : 3306,
    'userid' : 'root',
    'passwd' : '123456',
    'database' : 'geolite2'
}

geolite2_url = 'http://geolite.maxmind.com/download/geoip/database/GeoLite2-City-CSV.zip'

provider_url = [
    #('Afrinic', 'http://ftp.apnic.net/stats/afrinic/delegated-afrinic-extended-latest'),
    ('Apnic', 'http://ftp.apnic.net/stats/apnic/delegated-apnic-extended-latest'),
    #('Arin', 'http://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest'),
    #('Iana', 'http://ftp.apnic.net/pub/stats/iana/delegated-iana-latest'),
    #('Lacnic', 'http://ftp.apnic.net/stats/lacnic/delegated-lacnic-extended-latest'),
    #('Ripencc', 'http://ftp.apnic.net/stats/ripe-ncc/delegated-ripencc-extended-latest')
]


_last_errmsg = ''
_dl_progress = 0.0
v = lambda a : a if '' != a else None


if 2 == sys.version_info.major:
    reload(sys)
    sys.setdefaultencoding('utf-8')
    PY2 = True
    PY3 = False
elif 3 == sys.version_info.major:
    PY2 = False
    PY3 = True


def _set_errmsg(msg):
    global _last_errmsg
    _last_errmsg = msg


def rpthook(downloaded, blocksize, total):
    global _dl_progress

    progress = float(downloaded * blocksize) / float(total)

    if progress >= _dl_progress:
        _dl_progress = _dl_progress + 0.1
        print('{0:.2%} '.format(progress))


def ipwhois(ip):
    def _whois(ip):
        s = None

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            if 0 != s.connect_ex(('whois.apnic.net', 43)):
                s.close()
                print('Remote server can not connect. (whois: {0})'.format(ip))
                return (False, None)

            s.sendall((ip + '\r\n').encode())
            s.setblocking(0)

            times = 3
            body = ''

            while times > 0:
                s_in, s_out, s_err = select.select([s], [], [], 10)  # wait 10s

                if not s_in or 0 == len(s_in):
                    times -= 1
                    print('Socket recv timedout! (whois: {0})'.format(ip))
                    continue

                d = s.recv(1024)
                
                if not d:
                    s.close()
                    return (True, body)
                
                body += d.decode(chardet.detect(d)['encoding'])
            
            print('Remote server has been lost! (whois: {0})'.format(ip))
            s.close()
        except:
            print('Socket error! (whois: {0})'.format(ip))
            s.close()
        return (False, None)
    # -- End _whois() --

    r = False
    w = ''
    t = 5

    while not r and t > 0:
        (r, w) = _whois(ip)

        if not r:
            t -= 1
            print('IP whois error, retrying.')
            time.sleep(1.0)

    return w


def geolite2_download():
    global _dl_progress
    global geolite2_url

    print('Downloading data file.')

    try:
        _dl_progress = 0.0
        urlretrieve(geolite2_url, os.path.basename(geolite2_url), rpthook)
        return True
    except IOError as e:
        _set_errmsg('IO Error: {0}'.format(e.strerror))
    except HTTPError as e:
        _set_errmsg('HTTP Error: {0} {1}'.format(e.code, e.reason))
    except URLError as e:
        _set_errmsg('URL Error: {0}'.format(e.reason))

    return False


def download_delegated_file():
    global _dl_progress
    global provider_url

    for p in provider_url:
        try:
            print('Downloading {0}'.format(p[0]))
            _dl_progress = 0.0
            urlretrieve(p[1], os.path.basename(p[1]), rpthook)
        except IOError as e:
            print('IO Error: {0}'.format(e.strerror))
            sys.exit(e.errno)
        except HTTPError as e:
            print('HTTP Error: {0} {1}'.format(e.code, e.reason))
            sys.exit(e.errno)
        except URLError as e:
            print('URL Error: {0}'.format(e.reason))
            sys.exit(e.errno)
        
    return True


def file_extension(path):
  return os.path.splitext(path)[1]


def geolite2_loadcsv():
    global geolite2_url

    print('Loading CSV file.')

    name_mapper = {
        'GeoLite2-City-Blocks-IPv6' : 'blocks_ipv6',
        'GeoLite2-City-Blocks-IPv4' : 'blocks_ipv4',
        'GeoLite2-City-Locations-en' : 'locations_en',
        'GeoLite2-City-Locations-ja' : 'locations_ja',
        'GeoLite2-City-Locations-zh-CN' : 'locations_zh-CN',
        'GeoLite2-City-Locations-fr' : 'locations_fr',
        'GeoLite2-City-Locations-ru' : 'locations_ru',
        'GeoLite2-City-Locations-pt-BR' : 'locations_pt-BR',
        'GeoLite2-City-Locations-de' : 'locations_de',
        'GeoLite2-City-Locations-es' : 'locations_es'
    }

    with open(os.path.basename(geolite2_url), 'rb') as fp:
        zf = zipfile.ZipFile(fp)
        fl = filter(lambda fn: '.csv' == file_extension(fn).lower(),
                    zf.namelist())

        data = {
            'blocks' : {},
            'locations' : {}
        }

        for f in fl:
            d = StringIO(zf.read(f).decode('utf-8'))

            n = os.path.splitext(os.path.basename(f))[0]
            k = name_mapper[n].split('_')

            data[k[0]][k[1]] = csv.reader(d)

        return data

    return None


def parse_provider(ver_num):
    global provider_url
    
    vl = []
    
    for p in provider_url:
        print('Parsing {0}'.format(p))
        
        with open(os.path.basename(p[1]), 'rt') as fp:
            for line in fp:
                if '#' == line[0]:
                    continue
                
                rec = line.split('|')
                
                if 'IPV4' == rec[2].upper():
                    netmask = int(32 - math.log(int(rec[4]), 2))
                elif 'IPV6' == rec[2].upper():
                    netmask = int(rec[4])
                else:
                    continue
                
                ip = rec[3] + '/{0}'.format(netmask)

                try:
                    if PY3:
                        ip_range = ipaddress.ip_network(ip)
                    elif PY2:
                        ip_range = ipaddress.ip_network(ip.decode('utf-8'))
                except:
                    print('Error IP: {0}'.format(ip))
                    continue

                start_ip = ip_range[0]
                end_ip = ip_range[-1]

                whois = StringIO(ipwhois(ip))
                
                for wline in whois:
                    if wline[0] in ('%', ' ', '\r', '\n', ''):
                        continue
                    
                    nn = wline.split(':')

                    if 'netname' == nn[0]:
                        prov = nn[1].strip(' ').strip('\n')
                        break
                
                print('Whois: {0} {1} {2} {3} {4} {5} {6}'.format(ip, start_ip, end_ip, rec[0], rec[1], rec[2], prov))
                vl.append((ip, str(start_ip), str(end_ip), rec[0], rec[1], rec[2], prov, ver_num))
                    
        print('{0} Providers: {1}'.format(p[0], len(vl)))
        
    return vl


def get_mysql_conn():
    global mysql_config
    return pymysql.connect(host = mysql_config['host'],
                           user = mysql_config['userid'],
                           password = mysql_config['passwd'],
                           db = mysql_config['database'],
                           port = mysql_config['port'],
                           charset = 'utf8')


def close_mysql_conn(con):
    con.close()


def get_ver_num():
    print('Getting version number.')
    ver_num = -1

    con = get_mysql_conn()

    try:
        with con.cursor() as cursor:
            cursor.execute('INSERT INTO `t_version` (`createdate`) VALUES (NOW());')
            ver_num = con.insert_id()

        con.commit()
    except:
        print('Insert version to MySQL failed.')

    close_mysql_conn(con)

    return ver_num


def block_ip_save_mysql(con, blocks, ver_num):
    print('Saving blocks ip to MySQL.')
    try:
        vl = []

        for k in blocks:
            for i in blocks[k]:
                if 1 == blocks[k].line_num:
                    continue

                if PY3:
                    ip = ipaddress.ip_network(i[0])
                elif PY2:
                    ip = ipaddress.ip_network(i[0].decode('utf-8'))

                start_ip = ip[0]
                end_ip = ip[-1]

                vl.append((str(ip), k, str(start_ip), str(end_ip),
                          v(i[1]), v(i[2]), v(i[3]), v(i[4]), v(i[5]),
                          v(i[6]), v(i[7]), v(i[8]), v(i[9]), ver_num))

            print('Blocks {0}: {1}'.format(k, len(vl)))

        with con.cursor() as cursor:
            cursor.executemany(('INSERT INTO `t_blocks_ip` VALUES ('
                            '%s, %s, ip_to_d(%s), ip_to_d(%s), '
                            '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s'
                            ');'), vl)

        con.commit()
    except pymysql.MySQLError as e:
        print('Save blocks ip to MySQL failed,', e)


def loc_lang_save_mysql(con, locations, ver_num):
    print('Saving locations lang to MySQL.')
    try:
        vl = []

        for k in locations:
            for i in locations[k]:
                if 1 == locations[k].line_num:
                    continue

                t = [v(d) for d in i]
                t.append(ver_num)

                vl.append(tuple(t))

            print('Locations {0}: {1}'.format(k, len(vl)))

        with con.cursor() as cursor:
            cursor.executemany(('INSERT INTO `t_locations` VALUES ('
                                '%s, %s, %s, %s, %s, %s, %s, '
                                '%s, %s, %s, %s, %s, %s, %s'
                                ');'), vl)

        con.commit()
    except pymysql.MySQLError as e:
        print('Save locations lang to MySQL failed,', e)


def provider_save_mysql(con, vl):
    print('Saving providers to MySQL.')
    
    print('Providers: {0}'.format(len(vl)))

    try:
        with con.cursor() as cursor:
            cursor.executemany(('INSERT INTO `t_providers` VALUES ('
                                '%s, ip_to_d(%s), ip_to_d(%s), %s, %s, %s, %s, %s'
                                ');'), vl)
        
        con.commit()
    except pymysql.MySQLError as e:
        print('Save providers to MySQL failed,', e)


def switch_to_newest(con):
    print('Switch to newest version.')

    try:
        with con.cursor() as cursor:
            cursor.execute('SELECT `newest_version`();')
            result = cursor.fetchone()

        con.commit()
    except pymysql.MySQLError as e:
        print('Switch to newest version failed,', e)


def clear_old_version(con, ver_num):
    print('Cleaning outdated version.')
    try:
        with con.cursor() as cursor:
            cursor.execute('DELETE FROM `t_blocks_ip` WHERE ver_num < %s;', (ver_num))
            print('Clear blocks: {0}'.format(con.affected_rows()))

            cursor.execute('DELETE FROM `t_locations` WHERE ver_num < %s;', (ver_num))
            print('Clear locations: {0}'.format(con.affected_rows()))

            cursor.execute('DELETE FROM `t_providers` WHERE ver_num < %s;', (ver_num))
            print('Clear providers: {0}'.format(con.affected_rows()))

        con.commit()
    except:
        print('Clear data failed.')


def parse_and_save_providers(ver_num):
    vl = parse_provider(ver_num)

    con = get_mysql_conn()
    
    provider_save_mysql(con, vl)

    close_mysql_conn(con)


def geolite2_save_mysql(data):
    print('Saving to MySQL.')

    ver_num = get_ver_num()

    if ver_num > 0:
        print('New Version: {0}'.format(ver_num))
        
        parse_and_save_providers(ver_num)

        con = get_mysql_conn()

        block_ip_save_mysql(con, data['blocks'], ver_num)
        loc_lang_save_mysql(con, data['locations'], ver_num)
        switch_to_newest(con)
        clear_old_version(con, ver_num)

        close_mysql_conn(con)


def output_errmsg():
    global _last_errmsg
    print(_last_errmsg)


def app_exit(msg, code):
    print(msg)
    if 0 != code:
        output_errmsg()
    sys.exit(code)


if __name__ == '__main__':
    if not geolite2_download():
        app_exit('GeoLite2 data file download failed.', 1)

    if not download_delegated_file():
        app_exit('Download delegated file failed.', 1)

    ipdata = geolite2_loadcsv()

    if ipdata is None:
        app_exit('GeoLite2 load csv failed.', 1)

    geolite2_save_mysql(ipdata)

    app_exit('Done.', 0)

