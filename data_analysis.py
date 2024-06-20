import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

# Database connection details
DB_URI = 'mysql+pymysql://fifa:fifa@localhost:3306/fifa'  # Replace with your actual details

def connect_to_db(uri):
    try:
        engine = create_engine(uri)
        return engine
    except Exception as e:
        print(f'Error connecting to the database: {e}')
        raise

def fetch_data(engine):
    try:
        query = '''
        SELECT p.player_id, p.short_name, p.long_name, p.age, p.dob, p.height_cm, p.weight_kg, p.overall, p.potential, p.value_eur, 
               p.wage_eur, p.preferred_foot, p.international_reputation, p.weak_foot, p.skill_moves, p.work_rate, p.body_type, 
               p.real_face, p.release_clause_eur, p.player_tags, p.team_jersey_number, p.loaned_from, p.joined, p.contract_valid_until,
               t.team_name, n.nationality_name, pos.position_name
        FROM players p
        LEFT JOIN teams t ON p.team_id = t.team_id
        LEFT JOIN nationalities n ON p.nationality_id = n.nationality_id
        LEFT JOIN positions pos ON p.position_id = pos.position_id
        '''
        df = pd.read_sql(query, con=engine)
        return df
    except Exception as e:
        print(f'Error fetching data: {e}')
        raise

def clean_data(df):
    # Fill missing values with appropriate placeholders
    df['team_name'] = df['team_name'].fillna('Unknown')
    df['position_name'] = df['position_name'].fillna('Unknown')
    df['nationality_name'] = df['nationality_name'].fillna('Unknown')
    
    # Ensure 'overall' and other numerical columns are of type float
    df['overall'] = pd.to_numeric(df['overall'], errors='coerce')
    
    return df

def plot_nationality_distribution(df):
    plt.figure(figsize=(14, 8))
    nationality_count = df['nationality_name'].value_counts().head(20)  # Display top 20 nationalities
    sns.barplot(x=nationality_count.values, y=nationality_count.index, palette='cubehelix')
    plt.title('Top 20 Nationalities by Player Count')
    plt.xlabel('Number of Players')
    plt.ylabel('Nationality')
    plt.savefig('nationality_distribution.png')
    plt.show()

def main():
    # Connect to the database
    engine = connect_to_db(DB_URI)
    
    # Fetch data
    df = fetch_data(engine)
    
    # Debug prints to ensure data is fetched correctly
    print("Fetched DataFrame head:")
    print(df.head())
    print("Fetched DataFrame description:")
    print(df.describe())
    
    # Clean data
    df = clean_data(df)
    
    # Plot nationality distribution
    plot_nationality_distribution(df)

if __name__ == '__main__':
    main()
