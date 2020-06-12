"""
Provides processing functions for CRSP data
"""

# Note: CRSP data is very clean, no need to remove duplicates


def get_yearly_return(w, sf, data_frequency='monthly'):
    # Compute compounded yearly returns on monthly or daily stock data
    None


def delist_dummy(w):
    # Create a delist dummy
    None


def add_ret2anndates(w, file_original, file_crsp_daily, anndates, x_days)

# Need columns GVKEY, datetime, anndates

# Check if everything is alright(e.g. no return data in original dataset), otherwise raise exception

# Add return data to original dataset on "permno"

# Create empty dataframe with x days + 1 columns
df_ret = pd.DataFrame(columns=range(x_days+1))



df = pd.merge(file_original, file_crsp_daily["ret"], how='left',on='permno')

return df