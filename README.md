#### Overview
This project involves setting up a database to store FIFA player data, importing the data into the database, and performing data analysis to generate visualizations and statistics. The project uses Python, Pandas, SQLAlchemy, Matplotlib, and Seaborn to achieve these tasks.

#### Prerequisites
- Python 3.x
- MySQL database server
- Required Python packages: pandas, sqlalchemy, matplotlib, seaborn, pymysql

#### Setup Instructions

1. **Clone the Repository**
   ```bash
   git clone https://github.com/your-username/fifa-player-data-analysis.git
   cd fifa-player-data-analysis
   ```

2. **Install Required Packages**
   ```bash
   pip install pandas sqlalchemy matplotlib seaborn pymysql
   ```

3. **Download CSV Data**
   Download the CSV files from the [Kaggle FIFA 23 Complete Player Dataset](https://www.kaggle.com/datasets/stefanoleone992/fifa-23-complete-player-dataset):
   - `male_players.csv`
   - `male_teams.csv`

   Place these files in the project directory.

4. **Database Setup**
   Ensure MySQL is installed and running. Create a database for the project:
   ```sql
   CREATE DATABASE fifa;
   ```

5. **Update Database URI**
   Update the `DB_URI` in both `data_import.py` and `statistics.py` scripts to match your database credentials:
   ```python
   DB_URI = 'mysql+pymysql://fifa:fifa@localhost:3306/fifa'
   ```

#### Data Import

1. **Run the Data Import Script**
   ```bash
   python data_import.py
   ```

   This script will:
   - Connect to the MySQL database.
   - Create the necessary tables (`teams`, `nationalities`, `positions`, `players`).
   - Import data from `male_players.csv` and `male_teams.csv` into these tables.

#### Data Analysis

1. **Run the Data Analysis Script**
   ```bash
   python statistics.py
   ```

   This script will:
   - Connect to the MySQL database.
   - Fetch data from the database.
   - Clean the data.
   - Generate and save visualizations:
     - Distribution of Player Overall Ratings
     - Average Overall Rating by Player Position
     - Top 10 Teams by Average Player Rating
     - Top 20 Nationalities by Player Count

The generated plots will be saved as PNG files in the project directory.