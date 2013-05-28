
def get_driver(drivers, provider):
    """
    Get single  driver.
    
    @param drivers : Driver dict.
    @param provider : Id of provider.
    @type provider : L{libcloud.types.Provider} L{ulibcloud.ext.extProvider}

    """

    if provider in drivers : 
        mod_name, driver_name = drivers[provider]
        _mod = __import__(mod_name, globals(), locals(), [driver_name])
        return getattr(_mod, driver_name)

    raise AttributeError('Provider %s does not exist' % (provider))


