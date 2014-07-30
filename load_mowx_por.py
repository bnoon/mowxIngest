import sys

from dataset_defs import Station
from local_defs import data_path, miss_path, src_qual_flag

scale_map = dict(TMAX=10.,TMIN=10.,TAVG=10.,SRAD=100.)

def mon_len(yr,mn) :
    if (yr == 1900 or yr % 4 != 0) :
        return [0,31,28,31,30,31,30,31,31,30,31,30,31][mn]
    return [0,31,29,31,30,31,30,31,31,30,31,30,31][mn]

valid_flag = src_qual_flag + 0x0040

if __name__ == "__main__" :
    src_stns = sorted(sys.argv[1:])
    for fname in sys.argv[1:] :
        stn_id = fname[:6]
        stn = Station(stn_id)
        stn.open(False)
        cur_ym = None
        all_obs = []
        for lcnt,l in enumerate(open(fname)) :
            ym,elem,data = l.strip().split(',',2)
            # validate record
            if len(ym) != 6 :
                sys.stderr.write('%s: invalid line %d, bad year/mo\n'%(fname,lcnt))
                continue
            yr,mn = int(ym[:4]),int(ym[4:])
            if elem == 'MEAN' : elem = 'TAVG'
            if elem not in scale_map :
                sys.stderr.write('%s: invalid line %d, bad element\n'%(fname,lcnt))
                continue
            data = data.split(',')
            if len(data) != mon_len(yr,mn) :
                sys.stderr.write('%s: invalid line %d, wrong number of days\n'%(fname,lcnt))
                continue
            
            if (yr,mn) != cur_ym :
                if cur_ym != None :
                    all_obs.append([cur_ym,obs])
                cur_ym, obs = (yr,mn), {}
            scale = scale_map[elem]
            v_obs = obs.setdefault(elem,{})
            for idx, datum in enumerate(data) :
                if datum in ('M','') : v_obs[idx+1] = (0x8000,0,-1)
                else : v_obs[idx+1] = (int(round(float(datum)*scale)),valid_flag,24)
        if len(obs) > 0 : all_obs.append([cur_ym,obs])
        if len(all_obs) > 0 : stn.put_data(all_obs)
        stn.close()            
