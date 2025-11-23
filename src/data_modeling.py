from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import csv

class CassandraDataModel:
    def __init__(self, keyspace='sparkify'):
        self.cluster = Cluster(['127.0.0.1'])
        self.session = self.cluster.connect()
        self.keyspace = keyspace
        
    def create_keyspace(self):
        """Create keyspace for Sparkify data"""
        self.session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
            WITH replication = {{
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }}
        """)
        self.session.set_keyspace(self.keyspace)
        self.session.row_factory = dict_factory
        
    def create_tables(self):
        """Create all required tables for the queries"""
        
        # Table for query 1: Session playlist
        self.session.execute("""
            DROP TABLE IF EXISTS session_playlist
        """)
        self.session.execute("""
            CREATE TABLE session_playlist (
                session_id int,
                item_in_session int,
                artist text,
                song text,
                length float,
                PRIMARY KEY (session_id, item_in_session)
            )
        """)
        
        # Table for query 2: User session plays
        self.session.execute("""
            DROP TABLE IF EXISTS user_session_plays
        """)
        self.session.execute("""
            CREATE TABLE user_session_plays (
                user_id int,
                session_id int,
                item_in_session int,
                artist text,
                song text,
                first_name text,
                last_name text,
                PRIMARY KEY ((user_id, session_id), item_in_session)
            )
        """)
        
        # Table for query 3: Users by song
        self.session.execute("""
            DROP TABLE IF EXISTS users_by_song
        """)
        self.session.execute("""
            CREATE TABLE users_by_song (
                song text,
                user_id int,
                first_name text,
                last_name text,
                PRIMARY KEY (song, user_id)
            )
        """)
        
    def load_data(self, csv_file):
        """Load data from processed CSV into Cassandra tables"""
        
        with open(csv_file, 'r', encoding='utf8') as f:
            csv_reader = csv.DictReader(f)
            
            for row in csv_reader:
                try:
                    # Insert into session_playlist
                    self.session.execute("""
                        INSERT INTO session_playlist (session_id, item_in_session, artist, song, length)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (int(row['sessionId']), int(row['itemInSession']), row['artist'], 
                          row['song'], float(row['length']) if row['length'] else 0.0))
                    
                    # Insert into user_session_plays
                    self.session.execute("""
                        INSERT INTO user_session_plays (user_id, session_id, item_in_session, 
                                                      artist, song, first_name, last_name)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (int(row['userId']), int(row['sessionId']), int(row['itemInSession']),
                          row['artist'], row['song'], row['firstName'], row['lastName']))
                    
                    # Insert into users_by_song
                    self.session.execute("""
                        INSERT INTO users_by_song (song, user_id, first_name, last_name)
                        VALUES (%s, %s, %s, %s)
                    """, (row['song'], int(row['userId']), row['firstName'], row['lastName']))
                    
                except Exception as e:
                    print(f"Error inserting row: {e}")
                    continue
    
    def close(self):
        """Close database connections"""
        self.cluster.shutdown()
