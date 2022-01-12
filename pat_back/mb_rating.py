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
    def __init__(self, curve_id, df_mb: pd.DataFrame, **params):
        """Class to represent a rating model based on FLS.
           df_mb: ['c', 'b', 'g', 'cap'], 'ID' as index
        """
        self.curve_id = curve_id
        self.__mb_curves = df_mb
        # if self.__mb_curves.index.name != 'ID':
        #     self.__mb_curves.set_index('ID', inplace = True)
        # self.__mb_curves = self.__mb_curves.astype('float')

        # mask = self.__mb_curves.uTgammaMean.isna() | self.__mb_curves.limMean.isna()
        # if np.any(mask): 
        #     df = self.__mb_curves.loc[mask,['mu', 'w', 'tau', 'theta','beta', 'cap']]
        #     df['uTgammaMean'] = df['theta'] * np.exp(gammaln(
        #         (df['beta'] + 1) / df['tau']) - gammaln(df['beta'] / df['tau']))

        #     df['limMean'] = (df['mu'] * -np.expm1(-df['cap'] / df['mu']) * df['w']) + (
        #         (df['uTgammaMean'] * gamma.cdf((df['cap'] / df['theta']) ** df['tau'],
        #             (df['beta'] + 1) / df['tau']) +
        #             df['cap'] * gamma.cdf((df['cap'] / df['theta']) ** df['tau'],
        #                 df['beta'] / df['tau'])) * (1 - df['w']))

        #     self.__mb_curves.loc[mask,['uTgammaMean','limMean']] = df[['uTgammaMean','limMean']]
    
    def __mb_las(self, x, polLmt, TIV, b, g, acp1):
        pTIVa =np.minimum(np.where(x / acp1 > polLmt, polLmt, x) / TIV, acp1) / acp1
        return np.where( np.isnan(x) | (x <=0) | (TIV<=0), 0,
            np.where(x / acp1 >= TIV, 1, 
                np.where(b == 0 | g == 1, pTIVa,
                    np.where(b < 0, pTIVa ** -b,
                            np.where(b == 1 & g > 1, np.log1p((g - 1) * pTIVa) / np.log(g),
                                    np.where(b * g == 1 & g > 1, (1 - b ** pTIVa) / (1 - b),
                                            np.where(b > 0 & b != 1 & b * g != 1 & g > 1, 
                                                np.log(((g - 1) * b + (1 - b * g) * b ^ pTIVa) /(1 - b)) / np.log(b * g),
                                                np.nan
                                            )
                                    )
                            )
                    )
                )
            )
        )

    def calculate_las(self, DT: pd.DataFrame, ded_type ='Retains_Limit', addt_cvg = 0) -> pd.DataFrame:
        """
            DT: ['PolicyID', 'Limit', 'Retention', 'Participation', 'PolPrem', 'TIV', 'RatingGroup', 'Stack']
                When return add two columns: PolLAS, DedLAS
        """
        
        #? the below logic need to be cleaned up
        # ded = DEDDUCT_TYPE[ded_type]
        # DT = DT.fillna({'Retention': 0, 'Participation': 1, 'TIV': 0, 'RatingGroup': self.curve_id})
        # DT = DT.merge(self.__mb_curves, how='left', left_on='RatingGroup', right_index = True)

        # DT['cap1'] = addt_cvg + 1
        # DT.loc[DT['cap1'] <= 1, ['cap1']] = DT.loc[DT['cap1'] <= 1, 'cap']

        # DT['Exh'] = (DT.Limit + (DT.Retention if ded == DEDDUCT_TYPE.Retains_Limit else 0)).fillna(0)
        # DT['Policy'] = DT.Retention + np.maximum(DT.Exh - DT.Retention, 0) * (DT['Stack'].isna() * (DT['cap1']-1) + 1)
        # DT['EffLmt'] = np.minimum(DT.TIV, DT.Exh) * DT['cap1']
        
        # x = np.minimum(np.minimum(DT.Policy, DT.EffLmt)
        # DT['PolLAS'] = self.__mb_las(x, DT['mu'], DT['w'], DT['tau'], DT['theta'], 
        #     DT['beta'], DT['cap'], DT['uTgammaMean']).fillna(0)

        # x = np.minimum(np.minimum(DT.Retention, DT.EffLmt)
        # DT['DedLAS'] = self.__mb_las(x, DT['mu'], DT['w'], DT['tau'], DT['theta'], 
        #     DT['beta'], DT['cap'], DT['uTgammaMean']).fillna(0)

        # DT.drop(columns=['Exh'], inplace = True)
        # DT.drop(columns = self.__mb_curves.columns, inplace = True)
        # return DT
        pass
