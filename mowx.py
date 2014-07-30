import sys, os
from base_data_by_station import BaseDlyStation, DlyTimeVar, BaseMissingInfo, Base_vXvN, \
    BadUnits, UnsupportedVar, good_flag, miss_flag, not_reported
from local_defs import data_path, miss_path, src_qual_flag

network_id = 8
id_type = 2
zlog_formats = {'dly':'keyddata','meta':'keydmeta'}

# size and time are used in the zlog_util to write stream
#   size is the number of bytes for each datum (value,flag,time)
#   time is the len of the ymd(h) tuple
#   these are the defaults
vXvN = Base_vXvN({
    'TMAX': (1,8),
    'TMIN': (2,8),
    'MEAN': (4,7),
    'SRAD': (10,7),
    }, size=9, time=3)


class Keyed_Var(DlyTimeVar):
#VELR 1111 1122 2222
#  simple flag map
    flag = {
        '':  0x0040,
        ' ': 0x0040,
        'T': 0x0080,
        'A': 0x00C0,
        'S': 0x0100,
        }
    
    def __init__(self, station, var_name, var_desc, base_units):
        super(Keyed_Var,self).__init__(station, var_name, var_desc, base_units)
        station.vars[var_name] = self


class KeyedStation(BaseDlyStation) :
    dataset = 'keyed'
    stn_id_type = 'mowx'
    full_month = False
    std_vars = {
        'TMAX': (Keyed_Var,'Maximum Temperature','DegF'),    # (1,8),
        'TMIN': (Keyed_Var,'Minimum Temperature','DegF'),    # (2,8),
        'MEAN': (Keyed_Var,'Mean Daily Temperature','DegF'),          # (4,7),
        'SRAD': (Keyed_Var,'Total Radiation Horizontal Surface','MJ/m2'),               # (10,7),
        }
    limits = {}
    
    def __init__(self, stn_id):
        super(KeyedStation,self).__init__(stn_id)
        stn_path = self.stn_path = [stn_id[:2],stn_id[2:]+'.nc']
        self.nc_filename = os.path.join(data_path,*stn_path)
        self.miss_filename = os.path.join(miss_path,*stn_path)
        
    def create(self, var_cnt_hint=0):
        # check path
        base_path = os.path.join(data_path,self.stn_path[0])
        if not os.path.isdir(base_path) : os.mkdir(base_path)
        super(KeyedStation,self).create(var_cnt_hint)

    def create_missing(self, basetime=None) :
        # check path
        base_path = os.path.join(miss_path,self.stn_path[0])
        if not os.path.isdir(base_path) : os.mkdir(base_path)
        super(KeyedStation,self).create_missing(basetime)


class MissingInfo(BaseMissingInfo) :
    min_por_len = 3
    vXvN = vXvN
    all_vars = vXvN.keys()
    all_vars.sort()
    
    def get_station_list(self) :
        stns = []
        for state in sorted(os.listdir(data_path)) :
            stns.extend([state+s[:-3] for s in os.listdir(os.path.join(
                data_path,state)) if s.endswith('.nc')])
        return stns

    def get_missing_filename(self, stn_id) :
        stn_path = [stn_id[:2],stn_id[2:]+'.nc']
        return os.path.join(miss_path,*stn_path)
        
    def get_stns(self, stns=None) :
        if stns is None :
            stns = self.get_station_list()
        for s in self.extras :
            if s not in stns : stns.append(s)
        stns.sort()
        for s in stns :
            yield s, self.load_missing_info(s)
