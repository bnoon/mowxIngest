import sys, os
from base_data_by_station import BaseDlyStation, DlyTimeVar, BaseMissingInfo, Base_vXvN, \
    BadUnits, UnsupportedVar, good_flag, miss_flag, not_reported
from local_defs import data_path, miss_path, src_qual_flag

network_id = 20
id_type = 12
zlog_formats = {'dly':'mowxdata','meta':'mowxmeta'}

# size and time are used in the zlog_util to write stream
#   size is the number of bytes for each datum (value,flag,time)
#   time is the len of the ymd(h) tuple
#   these are the defaults
vXvN = Base_vXvN({
    'TMAX': (1,22),
    'TMIN': (2,22),
    'TAVG': (116,3),
    'SRAD': (70,4),
    }, size=9, time=3)


class MOwx_Var(DlyTimeVar):
#VELR 1111 1122 2222
#  simple flag map
    flag = {
        '':  0x0040,
        ' ': 0x0040,
        }
    
    def __init__(self, station, var_name, var_desc, base_units):
        super(MOwx_Var,self).__init__(station, var_name, var_desc, base_units)
        station.vars[var_name] = self


class MOwxStation(BaseDlyStation) :
    dataset = 'mowx'
    stn_id_type = 'mowx'
    full_month = False
    std_vars = {
        'TMAX': (MOwx_Var,'Maximum Temperature','0.1 DegF'),                  # (1,22),
        'TMIN': (MOwx_Var,'Minimum Temperature','0.1 DegF'),                  # (2,22),
        'TAVG': (MOwx_Var,'Mean Daily Temperature','0.1 DegF'),               # (116,3),
        'SRAD': (MOwx_Var,'Total Radiation Horizontal Surface','0.01 MJ/m2'), # (70,4),
        }
    limits = {}
    
    def __init__(self, stn_id):
        super(MOwxStation,self).__init__(stn_id)
        stn_path = self.stn_path = stn_id+'.nc'
        self.nc_filename = os.path.join(data_path,stn_path)
        self.miss_filename = os.path.join(miss_path,stn_path)

class MissingInfo(BaseMissingInfo) :
    min_por_len = 3
    vXvN = vXvN
    all_vars = vXvN.keys()
    all_vars.sort()
    
    def get_station_list(self) :
        stns = [s[:-3] for s in os.listdir(data_path) if s.endswith('.nc')]
        return stns

    def get_missing_filename(self, stn_id) :
        return os.path.join(miss_path,stn_id+'.nc')
        
    def get_stns(self, stns=None) :
        if stns is None :
            stns = self.get_station_list()
        for s in self.extras :
            if s not in stns : stns.append(s)
        stns.sort()
        for s in stns :
            yield s, self.load_missing_info(s)
