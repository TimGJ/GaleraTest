"""
Creates a corpus of data on the primary node (arthur)
"""
import logging
import argparse
import yaml
import sys
import random
import uuid
import mariadb

class ClusterSimulator:
    """
    Class which connects to and drives the various nodes in the simulation. There are four nodes each with a separate
    role:

    - primary: a MariaDB server which is initially an unclustered primary with a replication secondary. This node
      is responsible for creating the initial corpus of data and then switching to a clustered primary.
    - secondary: a MariaDB server which is initially an unclustered secondary. This node is responsible for
        replicating the primary and then switching to a clustered secondary.
    - peer: a MariaDB server which is initially a standalone server. This node is responsible for joining the cluster
      as a peer.
    - auditor: a MariaDB server which is initially a standalone server. This node is responsible for auditing the
      cluster.
    """

    def __init__(self, primary, secondary, peer, auditor):
        # Create the database connections and cursors
        self.priname, self.pricnx, self.pricur = self.create_connection(primary)
        logging.info(f'Connected to primary {self.priname} at {primary["host"]}')
        self.secname, self.seccnx, self.seccur = self.create_connection(secondary)
        logging.info(f'Connected to secondary {self.secname} at {secondary["host"]}')
        self.peername, self.peercnx, self.peercur = self.create_connection(peer)
        logging.info(f'Connected to peer {self.peername} at {peer["host"]}')
        self.audname, self.audcnx, self.audcur = self.create_connection(auditor)
        logging.info(f'Connected to auditor {self.audname} at {auditor["host"]}')

        # Get the names of the customers from the primary

        self.customers = self.get_customers()
        logging.info(f'Found {len(self.customers)} customers in primary {self.priname}')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):

        if self.pricnx:
            logging.info(f"Closing connection to primary {self.priname}")
            self.pricnx.commit()
            self.pricnx.close()
        if self.seccnx:
            logging.info(f"Closing connection to secondary {self.secname}")
            self.seccnx.commit()
            self.seccnx.close()
        if self.peercnx:
            logging.info(f"Closing connection to peer {self.peername}")
            self.peercnx.commit()
            self.peercnx.close()
        if self.audcnx:
            logging.info(f"Closing connection to auditor {self.audname}")
            self.audcnx.commit()
            self.audcnx.close()

    def create_connection(self, credentials, database='test'):
        """
        Creates a connection to a MariaDB server
        :param credentials: a dictionary of connection parameters
        :return: a connection object and cursor
        """
        name = credentials['name']
        connection = mariadb.connect(host=credentials['host'],user=credentials['user'],
                                     password=credentials['password'], database=database)
        cursor = connection.cursor()
        return name, connection, cursor

    def get_customers(self):
        """
        Gets the list of customers from the primary
        :return: dictionary of customers
        """

        sql = "SELECT id, name FROM customers"
        self.pricur.execute(sql)
        customers = {row[0]:row[1] for row in self.pricur.fetchall()}
        if len(customers) == 0:
            logging.critical("No customers found in primary database")
            sys.exit(1)
        return customers

    def insert_transactions(self, name, cursor, number=1):
        logging.info(f"Inserting {number} transactions into {name}")
        for i in range(number):
            customer = random.choice(list(self.customers.keys()))
            amount = random.randint(-10000,10000)/100
            uu = str(uuid.uuid4())
            sql = 'INSERT INTO transactions (node, uuid, customer_id, amount) VALUES (?, ?, ?, ?)'
            cursor.execute(sql, (name, uu, customer, amount))
            sql = 'INSERT INTO audit (node, uuid, customer_id, amount) VALUES (?, ?, ?, ?)'
            self.audcur.execute(sql, (name, uu, customer, amount))
            pass

if __name__ == '__main__':
    description = "Sets up an initial corpus of data for the simulation."
    epilog = "Copyright \N{COPYRIGHT SIGN} 2023 Snowy Consultants Limited"
    ap = argparse.ArgumentParser(description=description, epilog=epilog)
    ap.add_argument('-c', '--config', help='Path to configuration file', required=True)
    ap.add_argument('-d', '--debug', help='Enable debug logging', action='store_true')
    args = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format='%(asctime)s %(levelname)-7s %(message)s')

    logging.info(f"Reading configuration from {args.config}")
    try:
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        logging.critical(f"Configuration file {args.config} not found")
        print(f"Configuration file {args.config} not found")
        sys.exit(1)

    try:
        with ClusterSimulator(primary=config['primary'],
                     secondary=config['secondary'],
                     peer=config['peer'],
                     auditor=config['auditor']) as sim:
            sim.insert_transactions(sim.priname, sim.pricur, 1000)
    except (mariadb.OperationalError,mariadb.ProgrammingError, mariadb.NotSupportedError) as e:
        logging.critical(f"Unable to connect to database: {e}")
        print(f"Unable to connect to database: {e}")
        sys.exit(1)
