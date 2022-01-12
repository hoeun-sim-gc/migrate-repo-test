from cmath import tau
from datetime import datetime
from random import betavariate
from typing import List
from scipy.special import gammaln
from scipy.stats import gamma
import numpy as np
import pandas as pd

from .pat_flag import COVERAGE_TYPE, DEDDUCT_TYPE, PERIL_SUBGROUP, PSOLD_PERSP, RATING_TYPE


class PsoldRating:
    def __init__(self, curve_id, df_psold: pd.DataFrame, aoi_split: List, **params):
        """Class to represent a rating model based on PSOLD.
            df_psold: ['RG', 'EG', 'OCC', 'W1', 'W2', 'W...']
            aoi_split: Array with lower in i and upper in i + 1
        """
        self.curve_id = curve_id
        self.trend_from = datetime(2015, 12, 31) if self.curve_id == 2016 else datetime(2020, 12, 31)
        self.trend_factor = float(params['trend_factor']) if 'trend_factor' in params else (
            1.05 if self.curve_id == 2020 else 1.035)
        
        
        ##
        self.__psold_curves = df_psold.set_index(['EG', 'RG']).sort_index()
        self.__aoi_split = aoi_split
        
        self.__num_aoi, self.max_rg = self.__psold_curves.index.max()
        self.__num_mix = np.max([int(g[1:]) for g in self.__psold_curves.columns[np.where(self.__psold_curves.columns.str.match('W(0-9)*'))]])
        self.__mu = 10 ** (3 + np.arange(self.__num_mix) * 0.5)

    def __get_blending_weights(self, wgts, addt_cvg):
        """Fully vectorized numpy version. It takes the occupancy weight (OW) vector 
        and blends the component sets of 11 parameters by their proportion of the OW, 
        using the ISO credibility counts and the LAS's so that the result reflects the mix.

        Assume wgts is normalized
        """
        wgts /= np.sum(wgts)
        A = self.__psold_curves[[f'W{i}' for i in range(1, self.__num_mix + 1)]].to_numpy().reshape(self.__num_aoi, self.max_rg, -1)
        C = self.__psold_curves['OCC'].to_numpy().reshape(self.__num_aoi, self.max_rg)

        x = (self.__aoi_split[1:] * (addt_cvg + 1))[:, np.newaxis]
        AOI_LAS = self.__melas(x, A, self.__mu)

        idx = list(*np.where(wgts > 0))
        aoi_prem = np.sum(AOI_LAS[:, idx] * C[:, idx], axis=1)
        aoi_cnt = wgts * (aoi_prem[:, np.newaxis] / AOI_LAS)

        custum_cnts = np.sum(aoi_cnt, axis=1)
        custum_wgts = np.sum(A * aoi_cnt[:, :, np.newaxis], axis=1) / custum_cnts[:, np.newaxis]

        return (custum_wgts, custum_cnts)

    def __melas(self, x, w, mu):
        """Fully vectorized numpy verion of MEALS to calculate limited average severity of the mixed exponential distribution. 
            The leading dimensions of x and will be used as batch
            x: loss (..., n )
            w: weights (..., n, m), already normalized by i
            nu: (m, ) Adjusted for currency and trend
        """

        return (w * -np.expm1(-x[..., np.newaxis] @ (1/mu[np.newaxis, :]))) @ mu

    def calculate_las(self, DT: pd.DataFrame, df_wts: pd.DataFrame = None, df_hpr: pd.DataFrame = None, 
            curr_adj = 1.0, ded_type='Retains_Limit', addt_cvg=2,
            avg_acc_date=datetime(2022, 1, 1)) -> pd.DataFrame:
        """
            DT: ['PolicyID', 'Limit', 'Retention', 'Participation', 'PolPrem', 'TIV', 'RatingGroup', 'Stack']
                When return add two columns: PolLAS, DedLAS
            df_wts: ['RG', 'PremiumWeight']. If use HPR we need additional column 'HPRTable'
            df_hpr: ['Limit', 'Weight']
        """
        adjf = curr_adj * self.trend_factor ** ((avg_acc_date - self.trend_from).days / 365.25)
        if df_wts is not None:
            df_wts = df_wts.set_index('RG').sort_index()
        if df_hpr is not None:
            df_hpr.sort_values('Limit',inplace = True)

        ded = DEDDUCT_TYPE[ded_type]
        DT = DT.fillna({'Retention': 0, 'Participation': 1, 'TIV': 0})
        DT['Exh'] = (DT.Limit + (DT.Retention if ded == DEDDUCT_TYPE.Retains_Limit else 0)).fillna(0)
        DT['Policy'] = DT.Retention + np.maximum(DT.Exh - DT.Retention, 0) * (DT['Stack'].isna() * addt_cvg + 1)
        DT['EffLmt'] = np.minimum(DT.TIV, DT.Exh) * (addt_cvg + 1)
        DT.drop(columns=['Exh'], inplace = True)
       
        DT['AOIr'] = np.searchsorted(self.__aoi_split[1:-1], DT.TIV / adjf, side='left')
        m = len(DT)
        wgt = pd.DataFrame(data=np.zeros((m, self.__num_mix)), columns=[f'W{i}' for i in range(1, self.__num_mix + 1)])
        mask_rg = (DT.RatingGroup.isna() | (DT.RatingGroup <= 0) | (DT.RatingGroup > self.max_rg)).to_numpy()  # ! important: Have to convert to numpy, otherwise can mess up if DT is not in natrual order
        if np.any(mask_rg):
            w = df_wts.PremiumWeight.astype('float').to_numpy()
            wt, _ = self.__get_blending_weights(w, addt_cvg)
            wgt.loc[mask_rg] = wt[DT.loc[mask_rg, 'AOIr'], :]
            
        A = self.__psold_curves[[f'W{i}' for i in range(1, self.__num_mix + 1)]].to_numpy().reshape(self.__num_aoi, self.max_rg, -1)

        wgt.loc[~mask_rg] = A[DT.loc[~mask_rg, 'AOIr'], DT.loc[~mask_rg, 'RatingGroup'].astype('int') - 1, :]
        wgt = wgt.to_numpy()

        x = np.vstack((np.minimum(DT.Policy, DT.EffLmt).to_numpy(), np.minimum(DT.Retention, DT.EffLmt).to_numpy()))
        DT[['PolLAS', 'DedLAS']] = self.__melas(x, wgt, self.__mu*adjf).T

        if df_hpr is not None:
            wgt = pd.DataFrame(data=np.zeros((m, self.__num_mix)), columns=[f'W{i}' for i in range(1, self.__num_mix + 1)])
            if np.any(mask_rg):
                w = df_wts.groupby('HPRTable').agg({'PremiumWeight': sum}).reindex(
                    range(self.max_rg), fill_value=0).sort_index().PremiumWeight.astype('float').to_numpy()
                wt, _ = self.__get_blending_weights(w, addt_cvg)
                wgt.loc[mask_rg] = wt[DT.loc[mask_rg, 'AOIr'], :]
            wgt.loc[~mask_rg] = A[DT.loc[~mask_rg, 'AOIr'],
                    df_wts.HPRTable[DT.loc[~mask_rg, 'RatingGroup'].astype('int')] - 1, :]
            wgt = wgt.to_numpy()

            DT[['PolLASH', 'DedLASH']] = self.__melas(x, wgt, self.__mu*adjf).T
            DT['HPRWeight'] = np.interp(DT.TIV / adjf, df_hpr.Limit, df_hpr.Weight)
            DT['PolLAS'] = DT.HPRWeight * DT.PolLASH + (1 - DT.HPRWeight) * DT.PolLAS
            DT['DedLAS'] = DT.HPRWeight * DT.DedLASH + (1 - DT.HPRWeight) * DT.DedLAS

            DT.drop(columns=['PolLASH', 'DedLASH', 'HPRWeight'], inplace=True)

        return DT