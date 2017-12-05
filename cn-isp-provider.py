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


if 2 == sys.version_info.major:
    PY2 = True
    PY3 = False
elif 3 == sys.version_info.major:
    PY2 = False
    PY3 = True

provider = [
    #('Afrinic', 'http://ftp.apnic.net/stats/afrinic/delegated-afrinic-extended-latest'),
    ('Apnic', 'http://ftp.apnic.net/stats/apnic/delegated-apnic-extended-latest'),
    #('Arin', 'http://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest'),
    #('Iana', 'http://ftp.apnic.net/pub/stats/iana/delegated-iana-latest'),
    #('Lacnic', 'http://ftp.apnic.net/stats/lacnic/delegated-lacnic-extended-latest'),
    #('Ripencc', 'http://ftp.apnic.net/stats/ripe-ncc/delegated-ripencc-extended-latest')
]


_dl_progress = 0.0
def rpthook(downloaded, blocksize, total):
    global _dl_progress

    progress = float(downloaded * blocksize) / float(total)

    if progress >= _dl_progress:
        _dl_progress = _dl_progress + 0.1
        print('{0:.2%} '.format(progress))

def download_delegated_file():
    global _dl_progress
    global provider

    for p in provider:
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

def parse_provider(loc):
    global provider
    
    if type(loc) == list or type(loc) == tuple:
        loc = map(lambda l: l.upper(), loc)
        if PY3:
            loc = tuple(loc)
    elif type(loc) == str:
        loc = tuple([loc.upper()])
        
    vl = []
    
    for p in provider:
        print('Parsing {0}'.format(p))
        
        with open(os.path.basename(p[1]), 'rt') as fp:
            for line in fp:
                if '#' == line[0]:
                    continue
                
                rec = line.split('|')
                
                if rec[1].upper() in loc:
                    if 'IPV4' == rec[2].upper():
                        netmask = int(32 - math.log(int(rec[4]), 2))
                    elif 'IPV6' == rec[2].upper():
                        netmask = int(rec[4])
                    else:
                        continue
                    
                    ip = rec[3] + '/{0}'.format(netmask)

                    if PY3:
                        ip_range = ipaddress.ip_network(ip)
                    elif PY2:
                        ip_range = ipaddress.ip_network(ip.decode('utf-8'))

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
                    vl.append((ip, str(start_ip), str(end_ip), rec[0], rec[1], rec[2], prov))
                    
        print('{0} Providers: {1}'.format(p[0], len(vl)))
        
    return vl

def save_to_mysql(vl):
    global mysql_config
    
    print('Saving to MySQL.')
    
    con = pymysql.connect(host = mysql_config['host'],
                              user = mysql_config['userid'],
                              password = mysql_config['passwd'],
                              db = mysql_config['database'],
                              port = mysql_config['port'],
                              charset = 'utf8')
        
    try:
        with con.cursor() as cursor:
            cursor.executemany(('REPLACE INTO `t_providers` ('
                                '`networks`, `ip_start`, `ip_end`, `registry`, `country`, `ip_type`, `provider`'
                                ') VALUES (%s, ip_to_d(%s), ip_to_d(%s), %s, %s, %s, %s);'), vl)
        
        con.commit()
    except pymysql.MySQLError as e:
        print('Save providers to MySQL failed,', e)
    
    try:
        with con.cursor() as cursor:
            cursor.execute('DELETE FROM `t_providers` WHERE `last_time` < (SELECT `createdate` FROM `t_version` ORDER BY `ver_num` DESC LIMIT 1);')
            print('Clear providers: {0}'.format(con.affected_rows()))

        con.commit()
    except pymysql.MySQLError as e:
        print('Clear providers MySQL failed,', e)

    con.close()

def entry():
    if not download_delegated_file():
        print('Download delegated file failed.')
        sys.exit(1)
        
    vl = parse_provider(['CN'])
    save_to_mysql(vl)
    
    print('Done.')

if __name__ == '__main__':
    entry()
