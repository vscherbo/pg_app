#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    PG application
"""

import logging
import time

import psycopg2
import psycopg2.extras

LOG_FORMAT = '[%(filename)-21s:%(lineno)4s - %(funcName)20s()]\
 %(levelname)-7s | %(asctime)-15s | %(message)s'

class PGException(Exception):
    """ PGapp exception class """
    def __init__(self, message):
        super().__init__(message)
        self.message = message
        logging.warning('PGException')

class LoggingCursor(psycopg2.extensions.cursor):
    """ cursor with logging """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = None

    def initialize(self, logger):
        """ initialize a logger """
        self.logger = logger

    def fetchone(self):
        """ fetchone and logging """
        try:
            res = psycopg2.extensions.cursor.fetchone(self)
            self.logger.info(res)
        except Exception as exc:
            self.logger.error(f"{exc.__class__.__name__}: {exc}")
            raise
        return res

    def fetchmany(self, size):
        """ fetchmany and logging """

class PGapp():
    """ class for PG app
    """
    def __init__(self, pg_host, pg_user, pg_db=None, pg_port=5432):
        # logging.getLogger(__name__).addHandler(logging.NullHandler())
        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(logging.NullHandler())
        self.conn_settings = {
                "host": pg_host,
                "port": pg_port,
                "database": pg_db or pg_user,
                "user": pg_user
                }

        self.conn = None
        self.curs = None
        self.curs_dict = None
        self.lcurs = None

    def set_session(self, **kwargs):
        """ wrap psycopg2 set_session()
        """
        if self.conn:
            self.conn.set_session(**kwargs)

    def pg_connect(self, cursor_factory=psycopg2.extras.DictCursor, connect_timeout=3):
        """
        Try to connect to PG
        TODO: kwargs
        """
        logging.info("Trying connection to pg_host=%s as pg_user=%s", \
                self.conn_settings["host"], self.conn_settings["user"])
        res = False
        try:
            # password='XXXX' - .pgpass
            self.conn = psycopg2.connect(connection_factory=psycopg2.extras.LoggingConnection,
                    connect_timeout=connect_timeout,
                    **self.conn_settings)
            self.conn.initialize(self.logger)
            self.curs = self.conn.cursor()
            self.lcurs = self.conn.cursor(cursor_factory=LoggingCursor)
            self.lcurs.initialize(self.logger)
            self.curs_dict = self.conn.cursor(cursor_factory=cursor_factory)
            res = True
            logging.info('PG %s connected', self.conn_settings["host"])
        except psycopg2.Error as err:
            logging.error("Connection failed, ERROR=%s", err)
        else:
            res = True
        return res

    def wait_pg_connect(self, reconnect_period=5):
        """
        Loop until an connection to PG is available.
        """
        while not self.pg_connect():
            time.sleep(reconnect_period)

    def run_query(self, query, dict_mode=False):
        """ execute query
            does not fetch
        """
        if not self.conn:
            return -999
        try:
            if dict_mode:
                self.curs_dict.execute(query)
            else:
                self.curs.execute(query)
        except psycopg2.Error as exc:
            self.conn.rollback()
            logging.exception('PG error=%s', exc.pgcode)
            res = exc.pgcode
        else:
            res = 0
        return res

    def do_query(self, query, reconnect=False, dict_mode=False):
        """ execute query
            does not fetch
        """
        if not self.conn:
            if reconnect:
                self.wait_pg_connect()
            else:
                return False
        res = False
        try:
            if dict_mode:
                self.curs_dict.execute(query)
            else:
                self.curs.execute(query)
        except psycopg2.OperationalError as exc:
            if reconnect and exc.pgcode in (None, '57P01', '57P02', '57P03'):
                self.wait_pg_connect()
            else:
                logging.exception('PG OperationalError=%s', exc.pgcode)
        except psycopg2.Error as exc:
            self.conn.rollback()
            logging.exception('PG error=%s', exc.pgcode)
        else:
            res = True
        return res

    def copy_from(self, *args, **kwargs):
        """ run PG \\COPY command
        """
        res = 0  # failed
        loc_reconnect = kwargs.pop('reconnect', False)
        if not self.conn:
            if loc_reconnect:
                self.wait_pg_connect()
            else:
                return res
        try:
            self.curs.copy_from(*args, **kwargs)
            self.conn.commit()
            logging.info('\\COPY-from commited')
        except psycopg2.OperationalError as exc:
            if loc_reconnect and exc.pgcode in ('57P01', '57P02', '57P03'):
                self.wait_pg_connect()
                res = 2
            else:
                logging.exception('PG \\COPY-from OperationalError=%s', exc.pgcode)
        except psycopg2.Error:
            logging.exception('\\COPY-from failed! Rolling back')
            self.conn.rollback()
            #raise PGException('\\COPY failed')
        else:  # \COPY commited
            res = 1
        return res

    def copy_expert(self, cmd_copy, arg_io):
        """ run PG \\COPY command
        """
        res = False
        try:
            self.curs.copy_expert(cmd_copy, arg_io)
            self.conn.commit()
            logging.info('\\COPY-expert commited')
        except psycopg2.OperationalError:
            logging.exception('\\COPY-expert command')
        except psycopg2.Error:
            logging.exception('\\COPY-expert failed! Rolling back')
            self.conn.rollback()
            #raise PGException('\\COPY failed')
        else:  # \COPY commited
            res = True
        return res

    def pg_close(self):
        """ Close cursors and connection """
        if self.lcurs:
            self.lcurs.close()
        if self.curs:
            self.curs.close()
        if self.curs_dict:
            self.curs_dict.close()
        if self.conn:
            self.conn.close()

def main():
    """ just main
    """
    pg_app = PGapp('vm-pg-devel.arc.world', 'arc_energo')
    # password='XXXX' - .pgpass
    pg_app.wait_pg_connect()
    pg_app.set_session(autocommit=True)

    # while not pg_app.do_query('SELECT COUNT(*) FROM arc_constants;',
    # while not pg_app.do_query("SELECT * FROM arc_constants WHERE const_name='photo_path';",
    #        reconnect=True):
    #    time.sleep(3)

    pg_app.lcurs.execute("SELECT * FROM arc_constants WHERE const_name='photo_path';")

    logging.debug('lcurs=%s', pg_app.lcurs)
    data = pg_app.lcurs.fetchone()
    logging.info('data=%s', data)

if __name__ == '__main__':
    import os
    import sys
    LOG_DIR = './'
    PROGNAME = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    logging.basicConfig(filename=f'{LOG_DIR}/{PROGNAME}.log', format=LOG_FORMAT,
                        level=logging.DEBUG)
    logging.info('config read')
    main()
