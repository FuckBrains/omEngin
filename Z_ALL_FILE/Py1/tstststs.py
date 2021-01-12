import pandas as pd

def countif(col_as_range,criteria):
    # col_as_range can be list or daraframe series
    if isinstance(col_as_range,list):
        count = col_as_range.count(criteria)
        return count
    elif isinstance(col_as_range, pd.core.series.Series):
        col_range_list = col_as_range.values.tolist()
        count = col_range_list.count(criteria)
        return count
    else:
        return "none"

# way of calling - print(countif(df['Colname'],"value_to_check"))
# we call above countif function using loop on dataframe and can store the result into a new column as following.
def match(srcstr,list_as_range,start_from = False):
    try:
        if start_from == False or start_from == "First":
            if isinstance(list_as_range,list):
                indices = [i for i, x in enumerate(list_as_range) if x == srcstr]
                return indices[0]
            elif isinstance(list_as_range, pd.core.series.Series):
                col_range_list = list_as_range.values.tolist()
                indices = [i for i, x in enumerate(col_range_list) if x == srcstr]
                return indices[0]
            else:
                return "none"
        elif start_from == "Last":
            if isinstance(list_as_range,list):
                indices = [i for i, x in enumerate(list_as_range) if x == srcstr]
                ln = len(indices)
                return indices[ln-1]
            elif isinstance(list_as_range, pd.core.series.Series):
                col_range_list = list_as_range.values.tolist()
                indices = [i for i, x in enumerate(col_range_list) if x == srcstr]
                ln = len(indices)
                return indices[ln-1]
            else:
                return "none"
    except:
        return "NA"


df = pd.DataFrame({
    'column_1': ['g', 't', 'n', 'w', 'n', 'g']
})

print(match('n',df['column_1'],"Last"))

df = df.assign(new_column = "NA")
list_as_range = df['column_1'].values.tolist()   #column_1 is the column name (can be any column)
for i in range(len(df)):
    cell_value = df.loc[i,'column_1']   #column_1 is the column name (can be any column)
    df.loc[i,'new_column'] = countif(list_as_range, cell_value)   #calling above functions
#print(df)
