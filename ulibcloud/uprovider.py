from ulibcloud.utils.misc import get_driver as get_provider_driver
from ulibcloud.ext.ext_drivers import DRIVERS as EXTDRIVERS
from libcloud.storage.providers import DRIVERS as LDRIVERS



DRIVERS = dict(LDRIVERS, ** EXTDRIVERS)


def get_driver(provider) :
    return get_provider_driver(DRIVERS, provider)



def get_driver_list(provider_list):
    driver_list = []
    for p in provider_list :
        driver_list.append(get_driver(p))
    return driver_list

