import pandas as pd
import plotly.express as px
import sqlalchemy
from sqlalchemy import create_engine
from plotly.subplots import make_subplots
from base.Helpers.list_H import create_string_from_list
fin_engine = create_engine(str("postgresql://postgres:postgres@192.168.2.69:5432/finance"))
#fin_engine.execute('SET search_path TO finance')
import pandas as pd
import pandas as pd
from base.Helpers.df_H import add_timestamp
def groupby_agg(df, group_columns, value_columns,index=['date']):
    """
    Performs groupby aggregation on a DataFrame.

    Args:
        df (DataFrame): The input DataFrame.
        group_columns (list): List of column names for grouping.
        value_columns (list): List of column names for aggregation.

    Returns:
        DataFrame: The aggregated DataFrame.
    """
    agg_dfs = []  # List to store intermediate aggregated DataFrames

    for col in group_columns:
        # Perform groupby aggregation for each group column
        agg_df = df.groupby([col]+index).agg({
            **{value_col: [
                ('mean', 'mean'),
                ('std', 'std'),
                ('var', 'var'),
                ('median', 'median'),
                ('q25', lambda x: x.quantile(0.25)),
                ('q75', lambda x: x.quantile(0.75)),
                ('min', 'min'),
                ('max', 'max'),
                ('count', 'count')
            ] for value_col in value_columns}
        })


        agg_df.columns = [f'{col}_{agg}' if agg != '' else col for col, agg in agg_df.columns]
        agg_df.reset_index(inplace=True)
        # Flatten the column names by prefixing them with the group column name
        #agg_df.reset_index(inplace=True)
        print(agg_df)
        #3agg_df.drop(columns=['index'])
        agg_df['group_index']=agg_df[col]
        agg_df.drop(columns=col,inplace=True)

        agg_df['group_type']=col
        agg_dfs.append(agg_df)
        print(agg_df)
    # Concatenate the intermediate aggregated DataFrames along the 0 axis
    aggregated_df = pd.concat(agg_dfs, axis=0,ignore_index=True).reset_index(drop=True).set_index(['group_type','group_index','date'])

    return aggregated_df

#rename joins to categories
#make value cols into csonstant
def stockTableProfileJoin(table_name,engine,c_values=['sector','industry','country','currency','exchange'],val_cols= [],index_cols=['symbol','date'],pct=False):
    # Set up color scale for bars
    colors = px.colors.qualitative.Plotly
    if not pct:
        df = pd.read_sql('SELECT * FROM {}'.format(table_name),engine)[index_cols+val_cols]
    else:
        df = pd.read_sql('SELECT * FROM {}'.format(table_name), engine)[index_cols + val_cols].set_index(index_cols).pct_change().dropna()
    if val_cols==[]:
        val_cols=findValueCols(df)#.drop(index_cols)
    # Create a subplot with multiple rows and one column
    fig = make_subplots(rows=1, cols=len(c_values), shared_xaxes=True, vertical_spacing=0.1)
    tup_c = create_string_from_list(c_values)
    #for i, c in enumerate(c_values):
    # Select the required columns from the SQL table
    #print(pd.read_sql("SELECT * FROM fct_tickers_stock_profile",fin_engine).columns)
    select_query ="SELECT Symbol,{} FROM fct_tickers_stocks_profile".format(tup_c)
    result = pd.read_sql(select_query, engine).rename({'Symbol':'symbol'})
    print(result)
    # Merge the portfolio dataframe with the column values from the SQL table
    df = pd.merge(df, result, on='symbol')
    idx = [f for f in index_cols if f !='symbol']
    # Group by the column and calculate the total portfolio allocation for each group
    #grouped = df.groupby(c)['Current Position'].sum()
    print(df)
    #print(df.groupby(['sector']).agg({'environmentalScore':'sum'})))
    adf = groupby_agg(df,c_values,val_cols,index=idx)
    print(adf)
    end_name_0 = table_name.split()[-3:]
    end_name=create_string_from_list(end_name_0,delimiter='_')
    print(adf)
    #adf.to_sql('vw_cat_'+end_name,fin_engine,index=True,if_exists='replace')
    print('Categorical View made for table {}'.format(table_name))

# Need to fix profile
# Symbol -> symbol
def findValueCols(df,indxe_vals=['symbol','date']):
    a=list(df.columns)
    for d in indxe_vals:
        a.remove(d)
    return a
# ESG Grouping
#stockTableProfileJoin('dim_tickers_stocks_esg',engine=fin_engine,val_cols=['environmentalScore','socialScore','governanceScore','ESGScore'])
#stockTableProfileJoin('dim_tickers_stocks_prc_daily',engine=fin_engine,val_cols=['close'],pct=True,index_cols=['Date','symbol'])
stockTableProfileJoin('dim_ticker_stocks_hst_employees_fmp',engine=fin_engine,index_cols=['symbol','filing_date','period_of_report'],val_cols=['employee_count'])