"""
Provides processing functions for IBES data
"""


def numanalys(w, file_det):
    """Return the number of analysts for each (itic, fpedats)."""
    None


def _adjustmentfactors(w, file_adj):
    """ Create an adjustment factor table with a start and end date
    for each factor."""
    None

def consensus(w, file_det, file_adjfactors=None, file_adj=None):
    """Compute the consensus from individual analysts estimates."""
    # Open the det file with the necessary fields
    cols = ['itic']
    det = w.open_data(file_det, cols)
    # Opent the adjustment factors file
    if file_adjfactors is not None:
        adjfactors = w.open_data(file_adjfactors)
    elif file_adj is not None:
        # Need to compute the adustment factors
        adjfactors = _adjustmentfactors(file_adj)
    elif file_adj is None:
        raise Exception("Please provide either the adjustment factor file",
                        " or the adjustment file from IBES.")

# Fetch analysts forecast data
def analyst_forecasts(file_espsus,columns,key='ticker')

    # Add the necessary columns to process the data
    cols_req = key + ['fpedats','anntims','anntims_act','usfirm','curr_act','measure''actual','fpi']
    cols = cols_req + list(set(columns) - set(cols_req))

    # Open data
    df_aforecast = w.open_data(file_espsus, cols)

    # Filter the ibes data
    df_aforecast = df_aforecast.loc[df_aforecast['usfirm'].isin('1')])
    df_aforecast = df_aforecast.loc[df_aforecast['curr_act'].isin('USD')])
    df_aforecast = df_aforecast.loc[df_aforecast['measure'].isin('EPS')])
    df_aforecast = df_aforecast[df_aforecast['actual'].notna()]
    df_aforecast = df_aforecast.loc[df_aforecast['fpi'].isin(['6','7','8','9','N','O','P','Q','R','S','T','L','Y'])])

    # Extract year and month information
    df_aforecast['fpyr'] = df_aforecast['fpedats'].dt.year
    df_aforecast['fpemon'] = df_aforecast['fpedats'].dt.month

    # Convert 'anntims' and 'anntims_act' to datetime/timestamp?
    # df_aforecast['anntims'] = pd.to_datetime(df_aforecast['anntims'], format="%Y%m%d%H%M%S")

    return df_aforecast


# Fetch the management forecast data
def management_forecasts(file_guidance,columns,key='ticker')

    # Add the necessary columns to process the data
    cols_req = ['usfirm','curr','pdicity','measure','units']
    cols = cols_req + list(set(columns) - set(cols_req))

    # Open data
    df_mforecast = w.open_data(file_guidance, cols)

    # Filter the ibes data
    df_mforecast = df_mforecast.loc[df_mforecast['usfirm'].isin('1')])
    df_mforecast = df_mforecast.loc[df_mforecast['curr_act'].isin('USD')])
    df_mforecast = df_mforecast.loc[df_mforecast['pdicity'].isin('QTR')])
    df_mforecast = df_mforecast.loc[df_mforecast['measure'].isin('EPS')])
    df_mforecast = df_mforecast.loc[df_mforecast['units'].isin('P/S')])
    
    # Convert 'anntims' to datetime/timestamp?
    # df_ibes['anntims'] = pd.to_datetime(df_ibes['anntims'], format="%Y%m%d%H%M%S")

    return df_mforecast

# Merge the consensus and the statistics
def merge_cons_stat(file_ibes_consesus,file_ibes_stats)

    # Add the necessary columns to process the data
    cols_req = ['itic','fpedats']
    cols = cols_req + list(set(columns) - set(cols_req))

    # Open data
    df_ibes_consesus = w.open_data(file_ibes_consesus, cols)
    df_ibes_stats = w.open_data(file_ibes_stats, cols)

# Merge the two datasets on 'itic' and 'fpedats'
df_ibes = pd.merge(df_ibes_consesus, df_ibes_stats,on=['itic','fpedats'])

return df_ibes