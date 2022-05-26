import carrot 
import importlib
import inspect

__default_cdm_version = '5.3.1'
if 'cdm' in carrot.params:
    __cdm_version = carrot.params['cdm']
else:
    __cdm_version = __default_cdm_version

__cdm_version_split = '_'.join(__cdm_version.split('.'))

try:
    __cdm_tables = importlib.import_module(f'carrot.cdm.objects.versions.v{__cdm_version_split}')
except ModuleNotFoundError as e:
    raise ModuleNotFoundError(f'Cannot find CDM version {__cdm_version}, this does not exist in this package!') from e

__cdm_tables = {
    m[0]:getattr(__cdm_tables,m[0])
    for m in inspect.getmembers(
            __cdm_tables,
            inspect.isclass)
}


__cdm_object_map = {
    obj.name : obj
    for obj in __cdm_tables.values()
}
def get_cdm_tables():
    return __cdm_object_map

import sys
this = sys.modules[__name__]

for name,_class in __cdm_tables.items():
    setattr(this,name,_class)

from .. import decorators

__cdm_decorator_map = {
    name.replace("define_",""):getattr(decorators,name)
    for name in dir(decorators)
    if 'define' in name
}

def get_cdm_decorator(key):
    return __cdm_decorator_map[key]

def get_cdm_class(key):
    return __cdm_object_map[key]

from .common import DestinationTable, DataFormatter, FormatterLevel

