import pandas as pd
from sqlalchemy import create_engine, text
import logging

# Configure logging
logging.basicConfig(filename='data_import.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Database connection details
DB_URI = 'mysql+pymysql://fifa:fifa@localhost:3306/fifa'  # Replace with your actual details

def connect_to_db(uri):
    try:
        engine = create_engine(uri)
        logging.info('Connected to the database successfully')
        return engine
    except Exception as e:
        logging.error(f'Error connecting to the database: {e}')
        raise

def create_tables(engine):
    try:
        with engine.connect() as conn:
            # Create tables
            conn.execute(text('''
            CREATE TABLE IF NOT EXISTS teams (
                team_id INT PRIMARY KEY,
                team_name VARCHAR(100) NOT NULL,
                league_name VARCHAR(100),
                nationality_name VARCHAR(50),
                overall INT,
                attack INT,
                midfield INT,
                defence INT,
                transfer_budget_eur DECIMAL(15, 2),
                club_worth_eur DECIMAL(15, 2)
            );
            '''))

            conn.execute(text('''
            CREATE TABLE IF NOT EXISTS nationalities (
                nationality_id INT PRIMARY KEY,
                nationality_name VARCHAR(50) NOT NULL
            );
            '''))

            conn.execute(text('''
            CREATE TABLE IF NOT EXISTS positions (
                position_id INT PRIMARY KEY AUTO_INCREMENT,
                position_name VARCHAR(50) NOT NULL
            );
            '''))

            conn.execute(text('''
            CREATE TABLE IF NOT EXISTS players (
                player_id INT PRIMARY KEY,
                short_name VARCHAR(50) NOT NULL,
                long_name VARCHAR(100) NOT NULL,
                age INT,
                dob DATE,
                height_cm INT,
                weight_kg INT,
                overall INT,
                potential INT,
                value_eur INT,
                wage_eur INT,
                preferred_foot VARCHAR(10),
                international_reputation INT,
                weak_foot INT,
                skill_moves INT,
                work_rate VARCHAR(50),
                body_type VARCHAR(50),
                real_face VARCHAR(10),
                release_clause_eur INT,
                player_tags TEXT,
                team_id INT,
                nationality_id INT,
                position_id INT,
                team_jersey_number INT,
                loaned_from VARCHAR(100),
                joined DATE,
                contract_valid_until INT,
                FOREIGN KEY (team_id) REFERENCES teams(team_id),
                FOREIGN KEY (nationality_id) REFERENCES nationalities(nationality_id),
                FOREIGN KEY (position_id) REFERENCES positions(position_id)
            );
            '''))
            logging.info('Tables created successfully')
    except Exception as e:
        logging.error(f'Error creating tables: {e}')
        raise

def import_data(engine):
    try:
        # Read the CSV files without specifying dtypes first
        df_players = pd.read_csv('male_players.csv', low_memory=False, nrows=1000)
        df_teams = pd.read_csv('male_teams.csv', low_memory=False)

        # Print column names for inspection
        print("Player columns:", df_players.columns)
        print("Team columns:", df_teams.columns)

        # Fill NA values with a placeholder
        df_players = df_players.fillna({
            'player_id': -1,
            'short_name': 'Unknown',
            'long_name': 'Unknown',
            'player_positions': 'Unknown',
            'overall': -1,
            'potential': -1,
            'nationality_id': -1,
            'nationality_name': 'Unknown',
            'club_team_id': -1,
            'team_jersey_number': -1,
            'loaned_from': 'Unknown',
            'joined': '1900-01-01',
            'contract_valid_until': -1
        })

        # List of columns to be converted to appropriate data types
        columns_to_convert = {
            'player_id': 'int',
            'short_name': 'str',
            'long_name': 'str',
            'player_positions': 'str',
            'overall': 'int',
            'potential': 'int',
            'nationality_id': 'int',
            'nationality_name': 'str',
            'club_team_id': 'int',
            'team_jersey_number': 'int',
            'loaned_from': 'str',
            'joined': 'str',  # Date conversion will be handled later
            'contract_valid_until': 'int'
        }

        # Convert only existing columns to appropriate data types
        for col, col_type in columns_to_convert.items():
            if col in df_players.columns:
                df_players[col] = df_players[col].astype(col_type)

        df_teams = df_teams.fillna({
            'team_id': -1,
            'team_name': 'Unknown',
            'league_name': 'Unknown',
            'nationality_name': 'Unknown',
            'overall': -1,
            'attack': -1,
            'midfield': -1,
            'defence': -1,
            'transfer_budget_eur': 0.0,
            'club_worth_eur': 0.0
        })

        df_teams = df_teams.astype({
            'team_id': 'int',
            'team_name': 'str',
            'league_name': 'str',
            'nationality_name': 'str',
            'overall': 'float',
            'attack': 'float',
            'midfield': 'float',
            'defence': 'float',
            'transfer_budget_eur': 'float',
            'club_worth_eur': 'float'
        })

        # Handle null values by dropping rows with null values in key columns
        df_players = df_players.dropna(subset=['long_name', 'short_name', 'player_positions', 'overall', 'potential'])
        df_teams = df_teams.dropna(subset=['team_name'])

        # Remove duplicate team_id entries
        df_teams = df_teams.drop_duplicates(subset=['team_id'])

        # Insert teams data
        teams = df_teams[['team_id', 'team_name', 'league_name', 'nationality_name', 'overall', 'attack', 'midfield', 'defence', 'transfer_budget_eur', 'club_worth_eur']]
        teams.to_sql('teams', con=engine, if_exists='append', index=False)

        # Insert nationalities data
        nationalities = df_players[['nationality_id', 'nationality_name']].drop_duplicates()
        nationalities.to_sql('nationalities', con=engine, if_exists='append', index=False)

        # Insert positions data
        positions = df_players[['player_positions']].drop_duplicates().rename(columns={'player_positions': 'position_name'})
        positions.to_sql('positions', con=engine, if_exists='append', index=False)

        # Convert 'joined' column to datetime if it exists
        if 'joined' in df_players.columns:
            df_players['joined'] = pd.to_datetime(df_players['joined'], errors='coerce').fillna(pd.Timestamp('1900-01-01'))

        # Merge to get foreign keys using club_team_id to match with team_id
        df_players = df_players.merge(teams, left_on='club_team_id', right_on='team_id', how='left')
        df_players = df_players.merge(nationalities, left_on='nationality_id', right_on='nationality_id', how='left')
        df_players = df_players.merge(positions, left_on='player_positions', right_on='position_name', how='left')

        # List of columns to insert into players table
        players_columns = ['player_id', 'short_name', 'long_name', 'age', 'dob', 'height_cm', 'weight_kg', 'overall', 'potential',
                           'value_eur', 'wage_eur', 'preferred_foot', 'international_reputation', 'weak_foot', 'skill_moves',
                           'work_rate', 'body_type', 'real_face', 'release_clause_eur', 'player_tags', 'team_jersey_number', 'loaned_from',
                           'joined', 'contract_valid_until', 'team_id', 'nationality_id', 'position_id']

        # Ensure only existing columns are selected for insertion
        players_columns = [col for col in players_columns if col in df_players.columns]

        players = df_players[players_columns]
        players.to_sql('players', con=engine, if_exists='append', index=False)

        logging.info('Data imported successfully')
    except Exception as e:
        logging.error(f'Error importing data: {e}')
        raise

def main():
    # Connect to the database
    engine = connect_to_db(DB_URI)
    
    # Create tables
    create_tables(engine)
    
    # Import data
    import_data(engine)

if __name__ == '__main__':
    main()
