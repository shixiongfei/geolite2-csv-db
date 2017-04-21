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

import sys
import os

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

import zipfile
import csv
import ipaddress
import pymysql


mysql_config = {
    'host' : '127.0.0.1',
    'port' : 3306,
    'userid' : 'root',
    'passwd' : '123456',
    'database' : 'geolite2'
}

geolite2_url = 'http://geolite.maxmind.com/download/geoip/database/GeoLite2-City-CSV.zip'


_last_errmsg = ''
_dl_progress = 0.0

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

def get_ver_num(con):
    print('Getting version number.')
    ver_num = -1

    try:
        with con.cursor() as cursor:
            cursor.execute('INSERT INTO `t_version` (`createdate`) VALUES (NOW());')
            ver_num = con.insert_id()

        con.commit()
    except:
        print('Insert version to MySQL failed.')

    return ver_num

v = lambda a : a if '' != a else None

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

def clear_old_version(con, ver_num):
    print('Cleaning outdated version.')
    try:
        with con.cursor() as cursor:
            cursor.execute('DELETE FROM `t_blocks_ip` WHERE ver_num < %s;', (ver_num))
            print('Clear blocks: {0}'.format(con.affected_rows()))

            cursor.execute('DELETE FROM `t_locations` WHERE ver_num < %s;', (ver_num))
            print('Clear locations: {0}'.format(con.affected_rows()))

        con.commit()
    except:
        print('Clear data failed.')

def geolite2_save_mysql(data):
    print('Saving to MySQL.')

    con = get_mysql_conn()

    ver_num = get_ver_num(con)

    if ver_num > 0:
        print('New Version: {0}'.format(ver_num))
        block_ip_save_mysql(con, data['blocks'], ver_num)
        loc_lang_save_mysql(con, data['locations'], ver_num)
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

    ipdata = geolite2_loadcsv()

    if ipdata is None:
        app_exit('GeoLite2 load csv failed.', 1)

    geolite2_save_mysql(ipdata)

    app_exit('Done.', 0)

