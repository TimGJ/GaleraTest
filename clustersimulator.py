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

class ClusterNode:
    """
    Simple class to represent a node in the cluster which groups the host name, db connection and cursor
    """
    def __init__(self, name, connection, cursor):
        self.name = name
        self.connection = connection
        self.cursor = cursor

    def __repr__(self):
        return f"ClusterNode: {self.name}"
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
        self.primary = self.create_connection(primary)
        logging.info(f"Connected to primary {self.primary.name}")
        self.secondary = self.create_connection(secondary)
        logging.info(f"Connected to secondary {self.secondary.name}")
        self.peer = self.create_connection(peer)
        logging.info(f"Connected to peer {self.peer.name}")
        self.auditor = self.create_connection(auditor)
        logging.info(f"Connected to auditor {self.auditor.name}")

        # Get the names of the customers from the primary

        self.customers = self.get_customers()
        logging.info(f'Found {len(self.customers)} customers in primary {self.primary.name}')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):

        if self.primary.connection:
            logging.info(f"Closing connection to primary {self.primary.name}")
            self.primary.connection.commit()
            self.primary.connection.close()
        if self.secondary.connection:
            logging.info(f"Closing connection to secondary {self.secondary.name}")
            self.secondary.connection.commit()
            self.secondary.connection.close()
        if self.peer.connection:
            logging.info(f"Closing connection to peer {self.peer.name}")
            self.peer.connection.commit()
            self.peer.connection.close()
        if self.auditor.connection:
            logging.info(f"Closing connection to auditor {self.auditor.name}")
            self.auditor.connection.commit()
            self.auditor.connection.close()

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
        return ClusterNode(name, connection, cursor)

    def get_customers(self):
        """
        Gets the list of customers from the primary
        :return: dictionary of customers
        """

        sql = "SELECT id, name FROM customers"
        self.primary.cursor.execute(sql)
        customers = {row[0]:row[1] for row in self.primary.cursor.fetchall()}
        if len(customers) == 0:
            logging.critical("No customers found in primary database")
            sys.exit(1)
        return customers

    def insert_transactions(self, target, number=1):
        logging.info(f"Inserting {number} transactions into {target.name}")
        for i in range(number):
            customer = random.choice(list(self.customers.keys()))
            amount = random.randint(-10000,10000)/100
            uu = str(uuid.uuid4())
            sql = 'INSERT INTO transactions (node, uuid, customer_id, amount) VALUES (?, ?, ?, ?)'
            target.cursor.execute(sql, (target.name, uu, customer, amount))
            sql = 'INSERT INTO audit (node, uuid, customer_id, amount) VALUES (?, ?, ?, ?)'
            self.auditor.cursor.execute(sql, (target.name, uu, customer, amount))
            pass

