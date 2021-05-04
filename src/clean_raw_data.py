import os
import pandas as pd



def clean_dataset(csv_filepath):
    """Performs basic cleaning of Austin Animal Center Intake/Outcome datasets

    Args:
        csv_filepath (str): A file path pointing to the raw version of either the Intake or Outcome file

    Returns:
        pandas.DataFrame: A cleaned pandas DataFrame version of the raw CSV data file
        
    Cleaning Steps:
        1. Converts column headers to lower-kebab-case
        2. Fills NaN values in the "sex-upon-intake/outcome" fields with 'Unknown'
        3. Converts "datetime" and "date-of-birth" fields to normalized datetime dtypes
        4. Drops "monthyear" and "intake-location" fields
        5. Drops duplicated entries (same animal, same day)
        6. Drops records with NaN values in "outcome-type" field
    
    """
    df = pd.read_csv(csv_filepath)
    
    # UNIVERSAL CHANGES
    # convert column headers to lower-kebab-case
    df.columns = df.columns.str.lower().str.replace(' ', '-')
    
    # fillna on missing sex information
    sex_col = [col for col in df.columns if col[:3].lower() == 'sex'][0]
    df[sex_col].fillna('Unknown', inplace=True)

    # convert datetime dtypes
    df['datetime'] = pd.to_datetime(df['datetime']).dt.normalize()

    # drop unnecessary columns
    df.drop(columns=['monthyear'], inplace=True)

    # drop duplicated records based on animal-id and datetime
    df.drop_duplicates(subset=['animal-id', 'datetime'], inplace=True)

    # INTAKE-SPECIFIC CHANGES
    if "intake" in csv_filepath.lower():
        df.drop(columns=['found-location'], inplace=True)
    
    # OUTCOME-SPECIFIC CHANGES
    elif "outcome" in csv_filepath.lower():
        df.dropna(subset=['outcome-type'], inplace=True)
        df['date-of-birth'] = pd.to_datetime(df['date-of-birth']).dt.normalize()

    return df 
    
raw_data_path = os.path.abspath('../data/01-raw-data')

for f in os.listdir(raw_data_path):
    if 'intake' in f.lower():
        clean_intakes = clean_dataset(os.path.join(raw_data_path, f))
    elif 'outcome' in f.lower():
        clean_outcomes = clean_dataset(os.path.join(raw_data_path, f))

clean_intakes.to_csv(r'../data/02-intermediate-data/clean_intakes.csv', index=False)
clean_outcomes.to_csv(r'../data/02-intermediate-data/clean_outcomes.csv', index=False)