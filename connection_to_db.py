import mysql.connector # mySQL driver which allows python to communitcate with mySQL databases
import os # allows interaction with the os, including reading environment variables



class DatabaseConnection:

    """
    A class that handles database connections as well as simple operations
    """

    def __init__(self):
        """
        called when an instance of the class is created
        takes the creds loaded via .env and stores them as attributes of the objects (using .self)
        """        
        self.host = os.getenv("DB_HOST")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.database = os.getenv("DB_NAME")
        
        self.connection = None  #initially set as None (to be set later)      


    def connect(self):
        """
        method which attempts to make a connection to the mySQL server
        mysql.connector.connect() establishes the connection with the credidentials passed in
        returns a connection object
        """
        self.connection = mysql.connector.connect(
        host=self.host,
        user=self.user,
        password=self.password
    )
        
        return self.connection

    def create_database(self):
        """
        Method that creates the database (if it doesn't exist already)
        checks if there's a connection to the mySQL server - otherwise tries to connect
        creates cursor object to be able to execute commands in a database
        then, using the cursor, creates a database if one doesn't exist already - prints confirmation
        lastly connects to the specific database to start using it
        """
    
        if self.connection is None:
            self.connect()
        
        cursor = self.connection.cursor()
        
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")       
        print(f"Database '{self.database}' created or already exists")
        
        cursor.execute(f"USE {self.database}")
        print(f"Now using database: {self.database}")


    def close_connection(self):
        """
        method that closes the database connection.
        first checks if a connective is active and, if so, closes it and prints confirmation
        """
        if self.connection:
            self.connection.close()
            print("MySQL connection closed")