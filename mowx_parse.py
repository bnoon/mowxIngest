import os

from base_data_by_station import good_flag, miss_flag
from local_defs import data_path, miss_path, src_qual_flag

var_list = 'TMAX TMIN MEAN SRAD'.split()
obsT = 24;

def mon_len(yr,mn) :
    if (yr == 1900 or yr % 4 != 0) :
        return [0,31,28,31,30,31,30,31,31,30,31,30,31][mn]
    return [0,31,29,31,30,31,30,31,31,30,31,30,31][mn]

def parse_line(line) :
    info = line.split(',')
    sid,vname = info[0]
    obsT = '2400'
    return sid,vname,obsT,info[3:-1]

def filter_records(fname,sids=None) :
    sid = os.path.basename(fname)[:6]
    for line in open(fname,'U') :
        ym,vName,obsT,data = parse_line(line)
	yr,mn = int(ym[:4]), int(ym[4:6])
        if sid is None : continue
        if sids is not None and sid not in sids : continue
        if len(sid) != 6 :
            print "invalid station",fname,sid,vName
            continue
        yield sid,vName,(yr,mn),obsT,data
        
src_flg = src_qual_flag+good_flag

# Digs into Station/Var internals (vars, delta, the changes datastructure)
def add_month(stn, v_name, ym, obsT, vals) :
    scale = dict(TMAX=1.0,TMIN=1.0,AVGT=1.0,SOLR=1.0)[v_name]
    
    if v_name not in stn.vars :
        var = stn.vars[v_name] = stn.new_var(v_name)
        var.open()
    else :
        var = stn.vars[v_name]
        
    ym_delta = var.delta.setdefault(ym,{})
    for idx,val in enumerate(vals) :
        if 'M' in val or len(val) == 0 : datum = (0x8000,0,obsT)
        else :
            datum = (int(round(float(val)*scale)), src_flg+0x0040, obsT)
        ym_delta[idx+1] = datum
