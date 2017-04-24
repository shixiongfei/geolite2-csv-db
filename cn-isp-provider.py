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
import socket
import chardet
import pymysql

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
    w = ''
    
    while True:
        try:
            w = ''
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if 0 == s.connect_ex(('whois.apnic.net', 43)):
                s.sendall((ip + '\r\n').encode())
                
                while True:
                    d = s.recv(1024)
                    
                    if not d:
                        break
                    
                    w += d.decode(chardet.detect(d)['encoding'])
            else:
                print('IP whois failed.')
                    
            s.close()
            
            break
        except:
            print('IP whois error, retrying.')
            time.sleep(1.0)
            continue
        
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
                    
                    ip = rec[3] + '/{0}'.format(netmask)
                    whois = StringIO(ipwhois(ip))
                    
                    for wline in whois:
                        if wline[0] in ('%', ' ', '\r', '\n', ''):
                            continue
                        
                        nn = wline.split(':')

                        if 'netname' == nn[0]:
                            prov = nn[1].strip(' ').strip('\n')
                            break
                    
                    vl.append((ip, rec[0], rec[1], rec[2], prov))
                    
                    time.sleep(0.05)    # do not too fast.
                    
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
                                '`networks`, `registry`, `country`, `ip_type`, `provider`'
                                ') VALUES (%s, %s, %s, %s, %s);'), vl)
        
        con.commit()
    except pymysql.MySQLError as e:
        print('Save providers to MySQL failed,', e)
    
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
