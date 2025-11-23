class SparkifyQueries:
    def __init__(self, session):
        self.session = session
    
    def query_1_session_playlist(self, session_id=338, item_in_session=4):
        """Give me the artist, song title and song's length in the music app history 
        that was heard during sessionId = 338, and itemInSession = 4"""
        
        rows = self.session.execute("""
            SELECT artist, song, length 
            FROM session_playlist 
            WHERE session_id = %s AND item_in_session = %s
        """, (session_id, item_in_session))
        
        results = []
        for row in rows:
            results.append({
                'artist': row['artist'],
                'song': row['song'],
                'length': row['length']
            })
        return results
    
    def query_2_user_session_plays(self, user_id=10, session_id=182):
        """Give me only the following: name of artist, song (sorted by itemInSession) 
        and user (first and last name) for userid = 10, sessionid = 182"""
        
        rows = self.session.execute("""
            SELECT artist, song, first_name, last_name 
            FROM user_session_plays 
            WHERE user_id = %s AND session_id = %s
        """, (user_id, session_id))
        
        results = []
        for row in rows:
            results.append({
                'artist': row['artist'],
                'song': row['song'],
                'user_name': f"{row['first_name']} {row['last_name']}"
            })
        return results
    
    def query_3_users_by_song(self, song="All Hands Against His Own"):
        """Give me every user name (first and last) in my music app history 
        who listened to the song 'All Hands Against His Own'"""
        
        rows = self.session.execute("""
            SELECT first_name, last_name 
            FROM users_by_song 
            WHERE song = %s
        """, (song,))
        
        results = []
        for row in rows:
            results.append({
                'user_name': f"{row['first_name']} {row['last_name']}"
            })
        return results
