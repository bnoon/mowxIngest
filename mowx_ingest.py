#!/bin/env python

import sys, os, gzip,time
from optparse import OptionParser
from dataset_defs import Station, vXvN, zlog_formats
from zlog_util import ZlogByStn
from mowx_parse import filter_records, add_month
from local_defs import data_path, miss_path, zlog_path, upd_path, zlog_src

def print_err(e) :
    #if e.startswith('bad var') : return
    sys.stderr.write(e+'\n')

def processFiles(src_files) :
    cur_id = None
    for fname in src_files :
        for info in filter_records(fname) :
            stn_id,vName,ym,obsT,data = info
            if vName is None : continue
            if stn_id != cur_id :
                if cur_id is not None :
                    obs = stn.get_delta()
                    print obs
                    stn.clear_delta()
                    stn.chk_data(obs)
                    yield stn
                    stn.close()
                cur_id = stn_id
                stn = Station(stn_id)
                try :
                    stn.open()
                except :
                    pass # failure is ok
            add_month(stn, vName, ym, obsT, data)
    if cur_id is not None :
        obs = stn.get_delta()
        stn.clear_delta()
        stn.chk_data(obs)
        yield stn
        stn.close()


parser = OptionParser()
pao = parser.add_option
opt = pao('--zlog', '-z', action="store", dest="zlog_name")
opts, args = parser.parse_args()

if opts.zlog_name is None : parser.error("Need a zlog name")
if len(args) == 0 : parser.error("Need an input file name")

zfile = open(opts.zlog_name,'w')
upd_time = time.time()

zlog = ZlogByStn(zlog_formats['dly'],zlog_src,vXvN)
zlog.make_zlogs(processFiles(args), zfile, upd_time)
