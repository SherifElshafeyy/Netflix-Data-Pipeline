import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Float, Integer, String, NVARCHAR
from sqlalchemy.dialects import oracle
from sqlalchemy import text

# Create SQLAlchemy engine
engine = create_engine('oracle+cx_oracle://hr:hr@localhost')

# Query to select all data from the staging table
query = "SELECT * FROM SRC_NETFLIX_DATA"

# Load data into DataFrame using the engine
df = pd.read_sql(query, engine)

# Remove Nulls from df according to specific columns
def remove_nulls(df_uncleaned, columns_null):
    df_cleaned = df_uncleaned.dropna(subset=columns_null).copy()
    return df_cleaned


columns_null = ['title', 'genres', 'release_year', 'imdb_id', 'imdb_average_rating', 'imdb_num_votes']
df_cleaned = remove_nulls(df, columns_null)

# Replace Null Values in URL Column by "Unknown"
df_cleaned['url'] = df_cleaned['url'].fillna("Unknown")

# Convert Specific columns from Float to Integer
def convert_to_integer(df_nonulls, columns_float):
    for i in columns_float:
        df_nonulls[i] = df_nonulls[i].astype(int)
    return df_nonulls


columns_float = ['release_year', 'imdb_num_votes']
df_cleaned = convert_to_integer(df_cleaned, columns_float)

# Format string columns
df_cleaned['type'] = df_cleaned['type'].str.title()
df_cleaned['available_countries']=df_cleaned['available_countries'].str.upper()

# Sort df_cleaned By imdb_average_rating desc
df_cleaned = df_cleaned.sort_values(by=['imdb_average_rating'], ascending=False)

# SCD Type 1 Logic
# First, load existing data from the target table
existing_data = pd.read_sql("SELECT imdb_id FROM tgt_netflix_cleaned_data", engine)

# Identify records to update and insert
existing_ids = existing_data['imdb_id'].tolist()
to_update = df_cleaned[df_cleaned['imdb_id'].isin(existing_ids)]
to_insert = df_cleaned[~df_cleaned['imdb_id'].isin(existing_ids)]

# Update records
with engine.connect() as conn:

    with conn.begin() as transaction:
        for _, row in to_update.iterrows():
            # Execute the update query
            result = conn.execute(
                text("""
                UPDATE tgt_netflix_cleaned_data
                SET url = :url,
                    title = :title,
                    type = :type,
                    genres = :genres,
                    release_year = :release_year,
                    imdb_average_rating = :imdb_average_rating,
                    imdb_num_votes = :imdb_num_votes,
                    available_countries = :available_countries
                WHERE imdb_id = :imdb_id
                """),
                {
                    'url': row['url'],
                    'title': row['title'],
                    'type': row['type'],
                    'genres': row['genres'],
                    'release_year': int(row['release_year']),
                    'imdb_id': row['imdb_id'],
                    'imdb_average_rating': float(row['imdb_average_rating']),
                    'imdb_num_votes': int(row['imdb_num_votes']),
                    'available_countries': row['available_countries']
                }
            )
            # Check the result for rows affected
            if result.rowcount == 0:
                print(f"No rows updated for imdb_id: {row['imdb_id']}")
            else:
                print(f"Successfully updated record with imdb_id: {row['imdb_id']}")


# Insert new records
to_insert.to_sql('tgt_netflix_cleaned_data', con=engine, if_exists='append', index=False,
                 dtype={
                     'url': NVARCHAR(200),
                     'title': String(250),
                     'type': String(80),
                     'genres': String(80),
                     'release_year': Integer,
                     'imdb_id': String(30),
                     'imdb_average_rating': Float(),
                     'imdb_num_votes': Integer,
                     'available_countries': String(400)
                 })
