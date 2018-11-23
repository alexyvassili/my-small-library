""" Processing Daemon Template v 1.0
    Заготовка для скрипта, который забирает
    пакетами определенные позиции,обрабатывает их,
    и потом обновляет таблицу.
    Скрипт не подключен в pipeline, но сделать это несложно.

    Параметры:
        -o --once    -- обработать все возможные позиции, после чего завершить работу
        -l --logging -- выдавать сообщения о ходе выполнения (в обычном режиме - в консоль,
                        в режиме демона - лог файл
        -d --daemon  -- скрипт запускается как демон
"""

import logging
import argparse
import os
import sys
import traceback
import time

from events import log_event

from core import get_positions_ids, process_positions, load_positions, upgrade_table


# получаем аргументы командной строки
def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--once', action='store_true', help='Run script on all positions and exit')
    parser.add_argument('-l', '--logging', action='store_true', help='Logging level is info')
    parser.add_argument('-d', '--daemon', action='store_true', help='Start script as daemon')
    return parser


parser = create_parser()
namespace = parser.parse_args()


# Параметры выполнения и логирования
# Скрипт подключается к events.py и пишет логи в базу

MODE = "once" if namespace.once else "daily"

LOGGING_FORMAT = '[%(asctime)s] %(levelname).1s %(message)s'
LOGGING_LEVEL = logging.INFO if namespace.logging else logging.WARNING

SCRIPT_NAME = 'GZ223 KEYWORDS PREPROCESSING MODULE'
FILE_NAME = 'gz_keywords_preprocessing'
SCRIPT_PATH = '../{}/{}'.format(
    os.path.split(os.path.dirname(sys.argv[0]))[-1],
    os.path.basename(sys.argv[0]),)

EVENT_TEXT_CONFIG = {"event_text_prefix": "ANALYTICS GZ223: KEYWORDS PREPROCESSING",
                     "event_file": SCRIPT_PATH}


def main():
    logging.info('Started main function')
    t0 = time.time()
    while True:
        t1 = time.time()
        logging.info('Loading positions')

        positions_ids = get_positions_ids()

        logging.info('Getting {} positions'.format(len(positions_ids)))
        if len(positions_ids) > 0:
            logging.info('Loading positions')

            positions = load_positions(positions_ids)

            logging.info('Process positions')

            process_positions(positions)

            logging.info('Positions parsing is finished')
            logging.info('Upgrade table')

            upgrade_table(positions)

            t2 = time.time()
            log_event("Finished preprocessing %d items in \"%s\" MODE" %
                                                    (len(positions_ids), MODE),
                                                    EVENT_TEXT_CONFIG,
                                                    time_delta=t2 - t1)
            logging.info("Finished preprocessing %d items in %s" %
                                             (len(positions_ids), t2 - t1))
        else:
            if MODE == "daily":
                logging.info('Sleep')
                time.sleep(120 * 1000)
            elif MODE == "once":
                t3 = time.time()
                log_event("Finished preprocessing of all items in \"%s\" MODE" % MODE,
                                                                        EVENT_TEXT_CONFIG,
                                                                        time_delta=t3 - t0)
                logging.info("Script in once mode. All item was processed. Exiting")
                sys.exit(0)


def start_script():

    if namespace.daemon:
        logging.basicConfig(format=LOGGING_FORMAT, datefmt='%Y.%m.%d %H:%M:%S',
                            level=LOGGING_LEVEL, filename='/tmp/{}.log'.format(FILE_NAME))
    else:
        logging.basicConfig(format=LOGGING_FORMAT, datefmt='%Y.%m.%d %H:%M:%S', level=LOGGING_LEVEL)

    log_event("STARTED {}".format(SCRIPT_NAME), EVENT_TEXT_CONFIG)
    logging.info("STARTED {} ON MODE: {}".format(SCRIPT_NAME, MODE))

    try:
        main()
        log_event("FINISHED {}".format(SCRIPT_NAME), EVENT_TEXT_CONFIG)

    except BaseException as e:
        traceback.print_exc()
        log_event("ERROR IN {}: {}".format(SCRIPT_NAME, repr(e)), EVENT_TEXT_CONFIG)


if __name__ == "__main__":

    if namespace.daemon:
        import daemon
        from daemon import pidfile

        pidf = '/tmp/{}.pid'.format(FILE_NAME)
        with daemon.DaemonContext(
                working_directory=os.getcwd(),
                umask=0o002,
                pidfile=pidfile.TimeoutPIDLockFile(pidf)
        ) as context:
            start_script()
    else:
        start_script()
