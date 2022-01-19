from cmath import tau
from datetime import datetime
from random import betavariate
from typing import List
from scipy.special import gammaln
from scipy.stats import gamma
import numpy as np
import pandas as pd

from .pat_flag import COVERAGE_TYPE, DEDDUCT_TYPE, PERIL_SUBGROUP, PSOLD_PERSP, RATING_TYPE

class FlsRating:
    def __init__(self, curve_id, df_fls: pd.DataFrame, **params):
        """Class to represent a rating model based on FLS.
           df_fls: ['mu', 'w', 'tau', 'theta','beta', 'cap','uTgammaMean', 'limMean'], 'ID' as index
        """
        self.curve_id = curve_id
        self.__fls_curves = df_fls
        if self.__fls_curves.index.name != 'ID':
            self.__fls_curves.set_index('ID', inplace = True)
        self.__fls_curves = self.__fls_curves.astype('float')

        mask = self.__fls_curves.uTgammaMean.isna() | self.__fls_curves.limMean.isna()
        if np.any(mask): 
            df = self.__fls_curves.loc[mask,['mu', 'w', 'tau', 'theta','beta', 'cap']]
            df['uTgammaMean'] = df['theta'] * np.exp(gammaln(
                (df['beta'] + 1) / df['tau']) - gammaln(df['beta'] / df['tau']))

            df['limMean'] = (df['mu'] * -np.expm1(-df['cap'] / df['mu']) * df['w']) + (
                (df['uTgammaMean'] * gamma.cdf((df['cap'] / df['theta']) ** df['tau'],
                    (df['beta'] + 1) / df['tau']) +
                    df['cap'] * gamma.cdf((df['cap'] / df['theta']) ** df['tau'],
                        df['beta'] / df['tau'])) * (1 - df['w']))

            self.__fls_curves.loc[mask,['uTgammaMean','limMean']] = df[['uTgammaMean','limMean']]
    
    def __fls_las(self, x, mu, w, tau, theta, beta, cap, uTgammaMean):
        return (w * (mu * - np.expm1(-x / mu)
            ) + (1 - w) * (uTgammaMean * gamma.cdf((x / theta) ** tau, (beta + 1) / tau
                        ) + x * gamma.sf((x / theta) ** tau, beta / tau))) / (
                w * mu  * - np.expm1(-cap / mu
            ) + (1 - w) * (uTgammaMean * gamma.cdf((cap / theta) ** tau, (beta + 1) / tau
                        ) + cap * gamma.sf((cap / theta) ** tau, beta / tau)))
        
        
    
    def calculate_las(self, DT: pd.DataFrame, ded_type ='Retains_Limit', addt_cvg = None) -> pd.DataFrame:
        """
            DT: ['PolicyID', 'Limit', 'Retention', 'TIV', 'RatingGroup', 'Stack']
                When return add two columns: PolLAS, DedLAS
        """
        ded = DEDDUCT_TYPE[ded_type]
        DT = DT.fillna({'Retention': 0, 'TIV': 0, 'RatingGroup': self.curve_id})
        DT = DT.merge(self.__fls_curves, how='left', left_on='RatingGroup', right_index = True)
        
        DT['acp1'] = addt_cvg + 1 if addt_cvg else DT['cap']
        DT['Exh'] = (DT.Limit + (DT.Retention if ded == DEDDUCT_TYPE.Retains_Limit else 0))
        DT['Policy'] = DT.Retention + np.maximum(DT.Exh - DT.Retention, 0) * np.where(DT['Stack'].isna(), DT['acp1'], 1)
        DT['EffLmt'] = np.minimum(DT.TIV, DT.Exh) * DT['acp1']
              
        x = np.minimum(np.minimum(DT.Policy, DT.EffLmt) / DT.TIV, DT.cap)
        DT['PolLAS'] = self.__fls_las(x, DT['mu'], DT['w'], DT['tau'], DT['theta'], 
            DT['beta'], DT['cap'], DT['uTgammaMean'])*DT['TIV']

        x = np.minimum(np.minimum(DT.Retention, DT.EffLmt) / DT.TIV, DT.cap)
        DT['DedLAS'] = self.__fls_las(x, DT['mu'], DT['w'], DT['tau'], DT['theta'], 
            DT['beta'], DT['cap'], DT['uTgammaMean'])*DT['TIV']

        DT.drop(columns=['Exh'], inplace = True)
        DT.drop(columns = self.__fls_curves.columns, inplace = True)
        return DT
