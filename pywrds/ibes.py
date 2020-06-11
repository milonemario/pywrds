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

    def add_ret2anndates(w, file_original, file_crsp_daily, anndates, x_days)

        # Need columns GVKEY, datetime, anndates

        # Check if everything is alright(e.g. no return data in original dataset), otherwise raise exception



        # Add return data to original dataset on "permno"
        df = pd.merge(file_original, file_crsp_daily["ret"], how='left',on='permno')

        return df
