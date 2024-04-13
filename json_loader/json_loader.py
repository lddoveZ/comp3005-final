import os
import json
import psycopg2
from psycopg2.extras import execute_batch


DB_PARAMS = {
    'dbname': 'project_database',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost'
}


EVENTS_DIR = r'C:\Users\MSI\Desktop\data\data\events'
MATCHES_DIR = r'C:\Users\MSI\Desktop\data\data\matches'
LINEUP_DIR = r'C:\Users\MSI\Desktop\data\data\lineups'
COMP_DIR = r'C:\Users\MSI\Desktop\data\data'

conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()

def load_competition(directory, cur):
        for file in directory:
            if file.endswith('.json'):
                with open(file, 'r', encoding='utf-8') as json_file:
                    data = json.load(json_file)
                for competition in data:
                    cur.execute("""
                        INSERT INTO Seasons (season_id, season_name, competition_id)
                        VALUES (%s, %s,%s)
                        ON CONFLICT (season_id) DO NOTHING;
                    """, (competition['season_id'], competition['season_name'],competition['competition_id']))
                    cur.execute("""
                        INSERT INTO Competitions(competition_name, competition_id,country_name)
                        VALUES (%s, %s,%s)
                        ON CONFLICT (season_id) DO NOTHING;
                    """, (competition['competition_name'], competition['competition_id'],competition['country_name']))
                    
                    
        conn.commit()

load_competition(COMP_DIR, cur)

def load_lineup(directory, cur):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                with open(os.path.join(root, file), 'r', encoding='utf-8') as json_file:
                    data = json.load(json_file)
                for team in data:
                    cur.execute("""
                        INSERT INTO Teams (team_id, team_name)
                        VALUES (%s, %s)
                        ON CONFLICT (team_id) DO NOTHING;
                    """, (team['team_id'], team['team_name']))
                    
                    for player in team['lineup']:
                        cur.execute("""
                            INSERT INTO Players (player_id, player_name, team_id)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (player_id) DO NOTHING;
                        """, (player['player_id'], player['player_name'], team['team_id']))
    conn.commit()

load_lineup(LINEUP_DIR, cur)


def load_match_data(directory, cur):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                with open(os.path.join(root, file), 'r', encoding='utf-8') as json_file:
                        data = json.load(json_file)
                        for match in data:
                            cur.execute("""
                                INSERT INTO Competitions (competition_id, competition_name, country_name)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (competition_id) DO NOTHING;
                                """, (
                                match['competition']['competition_id'],
                                match['competition']['competition_name'],
                                match['competition']['country_name']
                                ))
                            cur.execute("""
                                INSERT INTO Seasons (season_id, season_name, competition_id)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (season_id) DO NOTHING;
                                """, (
                                match['season']['season_id'],
                                match['season']['season_name'],
                                match['competition']['competition_id']
                                ))
                            cur.execute("""
                                INSERT INTO Matches (match_id, home_team_id, away_team_id, season_id, match_date, home_score, away_score,competition_id)
                                VALUES (%s, %s, %s, %s, %s, %s, %s,%s)
                                ON CONFLICT (match_id) DO NOTHING;
                                """, (
                                match['match_id'],
                                match['home_team']['home_team_id'],
                                match['away_team']['away_team_id'],
                                match['season']['season_id'],
                                match['match_date'],
                                match['home_score'],
                                match['away_score'],
                                match['competition']['competition_id']
                                ))
                            cur.execute("""
                                INSERT INTO Teams (team_id,team_name)
                                VALUES (%s, %s)
                                ON CONFLICT (team_id) DO NOTHING;
                                """, (
                                match['home_team']['home_team_id'],
                                match['home_team']['home_team_name']
                                ))
                            cur.execute("""
                                INSERT INTO Teams (team_id,team_name)
                                VALUES (%s, %s)
                                ON CONFLICT (team_id) DO NOTHING;
                                """, (
                                match['away_team']['away_team_id'],
                                match['away_team']['away_team_name']
                                ))
    conn.commit()


load_match_data(MATCHES_DIR,cur)

def load_event(directory, cur):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                with open(os.path.join(root, file), 'r', encoding='utf-8') as json_file:
                        data = json.load(json_file)
                        for event in data:
                            cur.execute("""
                                INSERT INTO Event_Types (type_id, type_name)
                                VALUES (%s, %s)
                                ON CONFLICT (type_id) DO NOTHING;
                                """, (event['type']['id'], 
                                event['type']['name']
                            ))
                            cur.execute("""
                                INSERT INTO Teams (team_id, team_name)
                                VALUES (%s, %s)
                                ON CONFLICT (team_id) DO NOTHING;
                                """, (event['team']['id'], event['team']['name']
                            ))
                            if 'player' in event:
                                player_id = event['player']['id']
                            else:
                                player_id = None
                            
                            if 'shot' in event and 'first_time' in event['shot']:
                                first_time = event['shot']['first_time']
                            else:
                                first_time = None

                            if event['type']['id'] in {16}:
                                cur.execute("""
                                    INSERT INTO Events (event_id,type_id,player_id,team_id,xg,outcome,first_show)
                                    VALUES (%s,%s, %s,%s, %s,%s, %s)
                                    ON CONFLICT (event_id) DO NOTHING;
                                    """, (event['id'], event['type']['id'], player_id,event['team']['id'],event['shot']['statsbomb_xg'],event['shot']['outcome']['name'],first_time
                                ))
                            elif event['type']['id'] in {14}:
                                cur.execute("""
                                    INSERT INTO Events (event_id,type_id,player_id,team_id,dribbled_outcome)
                                    VALUES (%s,%s, %s,%s, %s)
                                    ON CONFLICT (event_id) DO NOTHING;
                                    """, (event['id'], event['type']['id'], player_id,event['team']['id'],event['dribble']['outcome']['name']
                                ))
                            
                            else:
                                cur.execute("""
                                    INSERT INTO Events (event_id,type_id,player_id,team_id)
                                    VALUES (%s,%s, %s,%s)
                                    ON CONFLICT (event_id) DO NOTHING;
                                    """, (event['id'], event['type']['id'], player_id,event['team']['id']
                                )) 

        

    conn.commit()

load_event(EVENTS_DIR,cur)



# Clean up
cur.close()
conn.close()
