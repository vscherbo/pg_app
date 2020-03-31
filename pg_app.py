#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    PG application
"""

import time
import logging
import pdb

import psycopg2
import psycopg2.extras

LOG_FORMAT = '[%(filename)-21s:%(lineno)4s - %(funcName)20s()]\
 %(levelname)-7s | %(asctime)-15s | %(message)s'

class PGException(Exception):
    """ PGapp exception class """
    def __init__(self, message):
        super(PGException, self).__init__(message)
        self.message = message
        logging.warning('PGException')

class PGapp():
    """ class for PG app
    """
    def __init__(self, pg_host, pg_user, pg_db=None, pg_port=5432):
        logging.getLogger(__name__).addHandler(logging.NullHandler())
        self.host = pg_host
        self.user = pg_user
        self.port = pg_port
        self.dbname = pg_db or pg_user
        self.conn = None
        self.curs = None
        self.curs_dict = None

    def set_session(self, **kwargs):
        """ wrap psycopg2 set_session()
        """
        if self.conn:
            self.conn.set_session(**kwargs)

    def pg_connect(self):
        """
        Try to connect to PG
        TODO: kwargs
        """
        logging.info("Trying connection to pg_host=%s as pg_user=%s", self.host, self.user)
        res = False
        try:
            # password='XXXX' - .pgpass
            self.conn = psycopg2.connect("host='{}' dbname='{}' \
user='{}' connect_timeout=3".format(self.host, self.dbname, self.user))
            self.curs = self.conn.cursor()
            self.curs_dict = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            res = True
            logging.info('PG %s connected', self.host)
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
            """
            logging.info("Trying connection to PG.")
            try:
                # password='XXXX' - .pgpass
                self.conn = psycopg2.connect("host='{}' dbname='{}' \
    user='{}'".format(self.host, self.dbname, self.user))
                self.curs = self.conn.cursor()
                self.curs_dict = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                logging.info('PG %s connected', self.host)
                break
            except psycopg2.Error:
                logging.warning("Connection failed. Retrying in 10 seconds")
                time.sleep(reconnect_period)
            """


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

    def do_query(self, query, reconnect=False):
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
            self.curs.execute(query)
        except psycopg2.OperationalError as exc:
            if reconnect and exc.pgcode in ('57P01', '57P02', '57P03'):
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

    while True:
        res = pg_app.run_query('SELECT COUNT(*) FROM arc_constants;')
        if res in ('57P01', '57P02', '57P03'):
            # admin_shutdown, crash_shutdown, cannot_connect_now
            # try reconnect in loop
            pg_app.wait_pg_connect()
        else:
            break
    data = pg_app.curs.fetchall()
    logging.info('data=%s', data[0])
    #pdb.set_trace()

if __name__ == '__main__':
    import os
    import sys
    LOG_DIR = './'
    PROGNAME = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    logging.basicConfig(filename='{}/{}.log'.format(LOG_DIR, PROGNAME), format=LOG_FORMAT,
                        level=logging.DEBUG)
    logging.info('config read')
    main()
