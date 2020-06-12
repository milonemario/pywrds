"""
Provides processing functions for IBES data
"""

"""
Import the necessary data from IBES
"""

# Fetch analysts forecast data
def fetch_analyst_forecasts(w, file_espsus,columns)

    # Add the necessary columns to process the data
    cols_req = ['fpedats','anntims','anntims_act','usfirm','curr_act','measure''actual','fpi']
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
def fetch_management_forecasts(w, file_guidance,columns,key='ticker')

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

# Fetch the adjustment factors
def fetch_adj_factors(w, file_adj,columns)

    # Add the necessary columns to process the data
    cols_req = ['ticker','spdates','adj','usfirm']
    cols = cols_req + list(set(columns) - set(cols_req))

    # Open data
    df_adj = w.open_data(file_adj, cols)

    # Filter the ibes data
    df_adj = df_adj.loc[df_adj['usfirm'].isin('1')])

    return df_adj

"""
/*******************************************************************************
 * *********************** IBES data - Some statistics *************************
 * Create a statistics table that includes:
 * - numanalys		Number of distinct analysts by firm-forecast period
 * 
 * Input tables:	a_forecast
 * Output table:	ibes_stats
 ******************************************************************************/
 """

def numanalys(w, file_det):
    """Return the number of analysts for each (itic, fpedats)."""
    # Assume data has itic, fpedats and analys
    # Open data
    df_det = w.open_data(file_det)

    # Solution 1
    #df_det = df_det.groupby(['itic','fpedats')['analys'].nunique()

    # Solution 2
    df_det = df_det.groupby(["itic", "number"])
    df_det = df_det.aggregate(len)
    df_det.reset_index().rename(columns={"analys": "num_analys"})

    # Rename dataframe
    df_ibes_stats = df_det

    return ibes_stats


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



'''
*******************************************************************************
 * ********************* IBES data - Adjustment factors ************************
 * Create an adjustment factor table with a start and end date for each factors.
 * Input tables:	adj
 * Output table:	_adj_factors
 *****************************************************************************
 '''

# Select the firms that have at least one split
def select_one_split(w, file_adj,columns)

    # Add the necessary columns to process the data
    cols_req = []
    cols = cols_req + list(set(columns) - set(cols_req))

    # Open data
    df_adj = w.open_data(file_adj, cols)

    # Count values and group by itic
    df_adj['itic_counts'] = df_adj.groupby('itic').count()

    # Select the data where itic_counts > 1
    df_itic_split = df_adj[df_adj.itic_counts > 1]

    return df_itic_split

# Create the new adjustment table
def new_adj_table(w, df)

    return df


"""
/*******************************************************************************
 * **************************** IBES data **************************************
 * Compute the consensus from individual analysts estimates
 * Input tables:	det_epsus, _adj_factors
 * Output table:	ibes2
 ******************************************************************************/
 """

# Keep the diluted EPS analysts estimates or the primary if no diluted available

def primary(w, file_a_forecasts, columns)

    # Add the necessary columns to process the data
    cols_req = ['itic', 'fpedats', 'pdf']
    cols = cols_req + list(set(columns) - set(cols_req))

    # Open data
    df_a_forecasts = w.open_data(file_a_forecasts, cols)

    # Count maximum and minimum and groupby
    df_a_forecasts['pdf_max'] = df_a_forecasts.groupby(['itic','fpedats'])['pdf'].transform(max)
    df_a_forecasts['pdf_min'] = df_a_forecasts.groupby(['itic','fpedats'])['pdf'].transform(min)

    # Filter data where pdf_max='P' and pdf_min='P'
    df_a_forecasts = df_a_forecasts.loc[df_a_forecasts['pdf_max'].isin('P')])
    df_a_forecasts = df_a_forecasts.loc[df_a_forecasts['pdf_min'].isin('P')])

    return df_a_forecasts

def diluted(w, file_a_forecasts, columns)

    # Add the necessary columns to process the data
    cols_req = ['itic', 'fpedats', 'pdf']
    cols = cols_req + list(set(columns) - set(cols_req))

    # Open data
    df_a_forecasts = w.open_data(file_a_forecasts, cols)

    # Filter data where pdf='D'
    df_a_forecasts = df_a_forecasts.loc[df_a_forecasts['pdf'].isin('D')])

    return df_a_forecasts



# Merge the consensus and the statistics
def merge_cons_stat(w,file_ibes_consesus,file_ibes_stats,columns)

    # Add the necessary columns to process the data
    cols_req = ['itic','fpedats']
    cols = cols_req + list(set(columns) - set(cols_req))

    # Open data
    df_ibes_consesus = w.open_data(file_ibes_consesus, cols)
    df_ibes_stats = w.open_data(file_ibes_stats, cols)

# Merge the two datasets on 'itic' and 'fpedats'
df_ibes = pd.merge(df_ibes_consesus, df_ibes_stats,on=['itic','fpedats'])

return df_ibes