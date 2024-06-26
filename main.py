import logging.config
import optparse
import configparser
import importlib
import pkgutil
import sys
import multiprocessing
import src.plugins
import traceback
import time

def iter_namespace(ns_pkg):
    return pkgutil.iter_modules(ns_pkg.__path__, ns_pkg.__name__ + ".")
def load_plugins():
    plugins = {}
    for finder, name, ispkg in iter_namespace(src.plugins):
        plugins[name] = {'module': importlib.import_module(name),
                         'module_path': finder.path}
        print(f"Imported: plugin {name}")
    return plugins
def main():
    parser = optparse.OptionParser()

    parser.add_option("--ConfigFile", dest="config_file",
                      help="Configuration file.", default=None)
    (options, args) = parser.parse_args()

    try:
        config_file = configparser.RawConfigParser()
        config_file.read(options.config_file)

        log_conf = config_file.get('logging', 'config_file')
        logging.config.fileConfig(log_conf)
        logger = logging.getLogger('data_ingest')
        logger.info("Log file opened.")

    except configparser.Error as e:
        traceback.print_exc(e)
        sys.exit(-1)


    plugin_modules = load_plugins()

    output_queue = multiprocessing.Queue()

    plugin_cnt = 0
    start_time = time.time()
    for plugin in plugin_modules:
        plugin_start_time = time.time()
        plugin_class = getattr(plugin_modules[plugin]['module'], 'DataIngest')
        plugin_obj = plugin_class(module_path=plugin_modules[plugin]['module_path'],
                                  output_queue=output_queue)
        logger.debug(f"Starting plugin: {plugin_obj.plugin_name}")
        if plugin_obj.initialize():
            plugin_obj.process_data()

        logger.debug(f"Finished plugin: {plugin_obj.plugin_name} in {time.time() - plugin_start_time}")
        plugin_cnt +=1

        logger.debug(f"Finished: {plugin_cnt} plugins in {time.time() - start_time}")

    return

if __name__ == "__main__":
    main()