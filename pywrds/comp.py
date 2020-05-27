"""
Provides processing functions for Compustat data
"""

# from pywrds import wrds
import pandas as pd


def get_naa_funda_us(w, file_funda, file_names, columns,
                     key=['gvkey', 'datadate']):
    # Return the consolidated financial reports of US domestic industrial
    # companies with standard accounting format.
    # Add the necessary columns to process the data
    cols_req = key + ['indfmt', 'datafmt', 'consol', 'popsrc']
    cols = cols_req + list(set(columns) - set(cols_req))
    # Open funda data
    df = w.open_data(file_funda, cols)
    # Open the identifiers (names)
    dn = w.open_data(file_names, cols)
    # Filter the funda data
    df = df[(df.indfmt == 'INDL') & (df.datafmt == 'STD') &
            (df.consol == 'C') & (df.popsrc == 'D')]
    # Merge the names informations
    df = pd.merge(df, dn, on='gvkey')
    # Check for duplicates on the key
    n_dup = df.shape[0] - df[key].drop_duplicates().shape[0]
    if n_dup > 0:
        print("Warning: The data contains {:} duplicates".format(n_dup))
    return df
