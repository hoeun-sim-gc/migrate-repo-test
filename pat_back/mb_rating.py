from cmath import tau
from datetime import datetime
from random import betavariate
from typing import List
from scipy.special import gammaln
from scipy.stats import gamma
import numpy as np
import pandas as pd

from .pat_flag import COVERAGE_TYPE, DEDDUCT_TYPE, PERIL_SUBGROUP, PSOLD_PERSP, RATING_TYPE

class MbRating:
    user_define = 58
    def __init__(self, curve_id, df_mb: pd.DataFrame):
        """Class to represent a rating model based on FLS.
           df_mb: ['b', 'g', 'cap'], 'ID' as index
        """
        self.curve_id = curve_id
        self.__mb_curves = df_mb
        if self.__mb_curves.index.name != 'ID':
            self.__mb_curves.set_index('ID', inplace = True)
        self.__mb_curves = self.__mb_curves.astype('float')
    
    def __mb_las(self, x, b, g, cap):
        return np.where(np.isnan(x) | (x <= 0), 0,
            np.where(x >= cap, 1, 
                np.where((b == 0) | (g == 1), x,
                    np.where(b < 0, x ** -b,
                        np.where((b == 1) & (g > 1), np.log1p((g - 1) * x) / np.log(g),
                            np.where((b * g == 1) & (g > 1), (1 - b ** x) / (1 - b),
                                np.where((b > 0) & (b != 1) & (b * g != 1) & (g > 1), 
                                    np.log(((g - 1) * b + (1 - b * g) * b ** x) /(1 - b)) / np.log(b * g),
                                    np.nan
                                )
                            )
                        )
                    )
                )
            )
        )

    def calculate_las(self, DT: pd.DataFrame, ded_type ='Retains_Limit', addt_cvg = None, custom_type = 1) -> pd.DataFrame:
        """
            DT: ['PolicyID', 'Limit', 'Retention', 'TIV', 'RatingGroup', 'Stack']
                When return add two columns: PolLAS, DedLAS
        """    
        ded = DEDDUCT_TYPE[ded_type]
        mask  = DT['RatingGroup'].notna()

        DT = DT.fillna({'Retention': 0, 'TIV': 0, 'RatingGroup': self.curve_id})
        DT = DT.merge(self.__mb_curves, how='left', left_on='RatingGroup', right_index = True)
        DT['acp1'] = addt_cvg + 1 if addt_cvg else DT['cap']

        if custom_type == 2:
            DT.loc[mask,['b']] = np.exp(3.1 - 0.15 * (1 + DT.RatingGroup) * DT.RatingGroup)
            DT.loc[mask,['g']] = np.exp((0.78 + 0.12 * DT.RatingGroup) * DT.RatingGroup)
            DT.loc[mask,['acp1']] = addt_cvg + 1 if addt_cvg else 1
        elif custom_type == 3:
            DT.loc[mask,['b']] = -DT.RatingGroup
            DT.loc[mask,['g']] = 0
            DT.loc[mask,['acp1']] = addt_cvg + 1 if addt_cvg else 1

        DT['Exh'] = (DT.Limit + (DT.Retention if ded == DEDDUCT_TYPE.Retains_Limit else 0))
        DT['Policy'] = DT.Retention + np.maximum(DT.Exh - DT.Retention, 0) * np.where(DT['Stack'].isna(), DT['acp1'], 1)
        DT['EffLmt'] = np.minimum(DT.TIV, DT.Exh) * DT['acp1']
              
        x = (np.minimum(DT.Policy, DT.EffLmt) / DT.TIV)
        DT['PolLAS'] = self.__mb_las(x, DT['b'], DT['g'], DT.cap) * DT.TIV
        
        x = (np.minimum(DT.Retention, DT.EffLmt) / DT.TIV)
        DT['DedLAS'] = self.__mb_las(x, DT['b'], DT['g'], DT.cap) * DT.TIV

        DT.drop(columns=['Exh'], inplace = True)
        DT.drop(columns = self.__mb_curves.columns, inplace = True)
        return DT
