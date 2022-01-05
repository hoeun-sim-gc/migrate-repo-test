import numpy as np 
import pandas as pd

from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
from pandas.api.types import is_float_dtype

def compare(df1, df2):
    cmp =pd.DataFrame(columns=['name','type', 'diff','df1_min','df2_min','df1_max','df2_max','df1_nan','df2_nan'])
    for c in df1.columns:
        if is_string_dtype(df1[c]):
            if np.all(df1[c].fillna(value="") != df2[c].fillna(value="")):
                cmp = cmp.append({'name': c,'type':df1[c].dtype,'diff':'NO'}, ignore_index=True)
            else:
                cmp = cmp.append({'name': c,'type':df1[c].dtype,'diff':'YES'}, ignore_index=True)
        elif is_float_dtype(df1[c]):
            df1.loc[df1[c].astype('float') > 1e20,[c]] = 1e20
            df2.loc[df2[c].astype('float') > 1e20,[c]] = 1e20
            dif = np.max(np.abs(df1[c].astype('float') - df2[c].astype('float')))
            # if dif > 1e-8:
            #     dif = np.max( 
            #             np.abs(df1[c].astype('float') - df2[c].astype('float')) /
            #             (np.maximum(np.abs(df1[c].astype('float')), np.abs(df2[c].astype('float'))) + 1e-10 )
            #         )            
            cmp = cmp.append({'name': c,'type':df1[c].dtype,'diff':dif,
                'df1_min':np.min(df1[c]),
                'df2_min':np.min(df2[c]),
                'df1_max':np.max(df1[c]),
                'df2_max':np.max(df2[c]),
                'df1_nan':np.sum(np.isnan(df1[c])),
                'df2_nan':np.sum(np.isnan(df2[c]))
            }, ignore_index=True)
        elif is_numeric_dtype(df1[c]):
            dif = np.max(np.abs(df1[c] - df2[c]))
            cmp = cmp.append({'name': c,'type':df1[c].dtype,'diff':dif,
                'df1_min':np.min(df1[c]),
                'df2_min':np.min(df2[c]),
                'df1_max':np.max(df1[c]),
                'df2_max':np.max(df2[c]),
                'df1_nan':np.sum(np.isnan(df1[c])),
                'df2_nan':np.sum(np.isnan(df2[c]))
            }, ignore_index=True)
        else:
            cmp = cmp.append({'name': c,'type':df1[c].dtype,'diff':'UNK'}, ignore_index=True)
        
    print(cmp)