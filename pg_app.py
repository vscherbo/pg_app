#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    A data exchange with Frontol 5
"""

import time
import logging
import psycopg2

LOG_FORMAT = '[%(filename)-21s:%(lineno)4s - %(funcName)20s()]\
 %(levelname)-7s | %(asctime)-15s | %(message)s'


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

    def set_session(self, **kwargs):
        """ wrap psycopg2 set_session()
        """
        self.conn.set_session(**kwargs)

    def wait_pg_connect(self, reconnect_period=5):
        """
        Loop until an connection to PG is available.
        """
        while True:
            logging.info("Trying connection to PG.")
            try:
                # password='XXXX' - .pgpass
                self.conn = psycopg2.connect("host='{}' dbname='{}' \
    user='{}'".format(self.host, self.dbname, self.user))
                self.curs = self.conn.cursor()
                logging.info('PG %s connected', self.host)
                break
            except psycopg2.Error:
                logging.warning("Connection failed. Retrying in 10 seconds")
                time.sleep(reconnect_period)


    def do_query(self, query):
        """ execute query
            does not fetch
        """
        res = False
        try:
            self.curs.execute(query)
        except psycopg2.OperationalError as exc:
            if exc.pgcode in ('57P01', '57P02', '57P03'):
                self.wait_pg_connect()
        except psycopg2.Error as exc:
            logging.exception('PG error=%s', exc.pgcode)
        else:
            res = True
        return res

def main():
    """ just main
    """
    pg_app = PGapp('vm-pg-devel.arc.world', 'arc_energo')
    # password='XXXX' - .pgpass
    pg_app.wait_pg_connect()
    pg_app.set_session(autocommit=True)


if __name__ == '__main__':
    LOG_DIR = './'
    logging.basicConfig(filename=LOG_DIR + '/ftd.log', format=LOG_FORMAT,
                        level=logging.DEBUG)
    logging.info('config read')
    main()
