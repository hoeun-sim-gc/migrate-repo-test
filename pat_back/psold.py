from datetime import datetime
from typing import List
import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame

from .pat_flag import CoverageCode, DedCode, SubGrpCode

class Psold:
    def __init__(self, df_psold: DataFrame, aoi_split: List, trend_ftr:float, trend_from:datetime):
        """Class to represent PSOLD curve utilities
            df_psold: ['RG', 'EG', 'OCC', 'W1', 'W2', 'W...']
            aoi_split: Array with lower in i and upper in i + 1
        """
        self.treeand_fctr = trend_ftr
        self.trend_from = trend_from
        self.aoi_split = aoi_split

        self.max_rg = df_psold.RG.max()
        self.num_aoi = df_psold.EG.max()
        self.num_mix = np.max([int(g[1:]) for g in df_psold.columns[np.where(
            df_psold.columns.str.match('W(0-9)*'))]])

        self.psold_a = df_psold.set_index(['EG', 'RG'])[[f'W{i}' for i in range(
            1, self.num_mix + 1)]].sort_index().to_numpy().reshape(self.num_aoi, self.max_rg, -1)
        self.psold_c = df_psold.set_index(['EG', 'RG'])['OCC'].sort_index(
        ).to_numpy().reshape(self.num_aoi, self.max_rg)

        self.mu = 1000 * np.power(10, np.arange(self.num_mix) * 0.5)

    def __blending(self, wgts, addt_cvg):
        """Fully vectorized numpy version. It takes the occupancy weight (OW) vector 
        and blends the component sets of 11 parameters by their proportion of the OW, 
        using the ISO credibility counts and the LAS's so that the result reflects the mix.

        Assume wgts is normalized
        """
        x = (self.aoi_split[1:] * (addt_cvg + 1))[:, np.newaxis]
        AOI_LAS = self.melas(x, self.psold_a, self.mu)

        idx = list(*np.where(wgts > 0))
        aoi_prem = np.sum(AOI_LAS[:, idx] *
                          self.psold_c[:, idx], axis=1)
        aoi_cnt = wgts * (aoi_prem[:, np.newaxis] / AOI_LAS)

        custum_cnts = np.sum(aoi_cnt, axis=1)
        custum_wgts = np.sum(
            self.psold_a * aoi_cnt[:, :, np.newaxis], axis=1) / custum_cnts[:, np.newaxis]

        return (custum_wgts, custum_cnts)
    
    def get_blending_weights(self, df_wts:DataFrame, addt_cvg:np.number, hpr:bool = False):      
        """ 
            df_wts: ['PremiumWeight'], with 'RG' as index. If hpr is true, we need additional column 'HPRTable'
        """

        df = df_wts.groupby('HPRTable').agg({'PremiumWeight':sum}).reindex(range(self.max_rg), fill_value=0) if hpr else df_wts
        
        df = df_wts.sort_index()
        wgts = df.PremiumWeight.astype('float').to_numpy()
        wgts /= np.sum(wgts)
        return self.__blending(wgts, addt_cvg)

    def melas(self, x, w, mu):
        """Fully vectorized numpy verion of MEALS to calculate limited average severity of the mixed exponential distribution. 
            The leading dimensions of x and will be used as batch
            x: loss (..., n )
            w: weights (..., n, m), already normalized by i
            nu: (m, ) Adjusted for currency and trend
        """

        return (w * (1 - np.exp(-x[..., np.newaxis] @ (1/mu[np.newaxis, :])))) @ mu

    def pat(self, DT:DataFrame, df_wts:DataFrame, df_hpr:DataFrame = None, 
            curr_adj = 1.0, ded_type = DedCode.Retains_Limit, addt_cvg =2,
            avg_acc_date = datetime(2022,1,1),
            inplace=True) -> DataFrame:
        """
            DT: ['PolicyID', 'Limit', 'Retention', 'Participation', 'PolPrem', 'LossRatio', 'TIV', 'RatingGroup', 'LocStack']
            df_wts: ['PremiumWeight'] with 'RG' as index. If use HPR we need additional column 'HPRTable'
            df_hpr: ['Limit', 'Weight']
        """

        trend_ftr = np.power(self.treeand_fctr, (avg_acc_date - self.trend_from).days / 365.25)
        adjf = curr_adj * trend_ftr

        if inplace: 
            DT.fillna({'Retention':0,'Participation':1,'TIV':0}, inplace=True)
        else:
            DT = DT.fillna({'Retention':0,'Participation':1,'TIV':0})

        DT['Exh'] = (DT.Limit + (DT.Retention if ded_type == DedCode.Retains_Limit else 0)).fillna(0)
        DT['Policy'] = DT.Retention + np.maximum(DT.Exh - DT.Retention, 0) * (DT['LocStack'].isna() * addt_cvg + 1) 
        DT['EffLmt'] = np.minimum(DT.TIV, DT.Exh) * (addt_cvg + 1) 
        DT['AOIr'] = np.searchsorted(self.aoi_split[1:-1], DT.TIV / adjf, side='left') # will be one less than the document which start from 1

        m = len(DT)
        wgt = pd.DataFrame(data = np.zeros((m, self.num_mix)), columns=[f'W{i}' for i in range (1, self.num_mix + 1)])
        mask_rg = ( DT.RatingGroup.isna() | (DT.RatingGroup <=0) | (DT.RatingGroup > self.max_rg)
                ).to_numpy() #! important: Have to convert to numpy, otherwise can mess up if DT is not in natrual order  
        if any(mask_rg):
            wt, _ = self.get_blending_weights(df_wts, addt_cvg) 
            wgt.loc[mask_rg] = wt[DT.loc[mask_rg,'AOIr'],:]
        wgt.loc[~mask_rg] = self.psold_a[DT.loc[~mask_rg,'AOIr'], DT.loc[~mask_rg,'RatingGroup'].astype('int') -1 ,:]
        wgt=wgt.to_numpy()

        x = np.vstack((np.minimum(DT.Policy,DT.EffLmt).to_numpy(),
                np.minimum(DT.Retention,DT.EffLmt).to_numpy()))
        DT[['PolLAS','DedLAS']] = self.melas(x,wgt,self.mu*adjf).T
        DT[['GuLimLAS','GuDedLAS']] = DT[['PolLAS','DedLAS']]

        if df_hpr is not None:
            wgt = pd.DataFrame(data = np.zeros((m, self.num_mix)), columns=[f'W{i}' for i in range (1, self.num_mix + 1)])
            if any(mask_rg):
                wt, _ = self.get_blending_weights(df_wts, addt_cvg, True) 
                wgt.loc[mask_rg] = wt[DT.loc[mask_rg,'AOIr'],:]
            wgt.loc[~mask_rg] = self.psold_a[DT.loc[~mask_rg,'AOIr'], df_wts.HPRTable[DT.loc[~mask_rg,'RatingGroup'].astype('int')] - 1 ,:]
            wgt=wgt.to_numpy()

            DT[['PolLASH','DedLASH']] =self.melas(x,wgt,self.mu*adjf).T
            DT['HPRWeight'] = np.interp(DT.TIV / adjf, df_hpr.Limit, df_hpr.Weight ) 
            DT['GuLimLAS'] = DT.HPRWeight * DT.PolLASH + (1 - DT.HPRWeight) * DT.PolLAS
            DT['GuDedLAS'] = DT.HPRWeight * DT.DedLASH + (1 - DT.HPRWeight) * DT.DedLAS
        
        DT['AllocPrem'] = (DT.GuLimLAS-DT.GuDedLAS)*DT.LossRatio
        
        sumLAS = DT.groupby('PolicyID').agg({'AllocPrem':'sum'}).rename(columns={'AllocPrem':'sumLAS'})
        if inplace:
            dt = DT[['PolicyID','PolPrem','AllocPrem']].reset_index()
            dt = dt.merge(sumLAS, on='PolicyID')
            dt['AllocPrem'] *= dt['PolPrem'] / dt['sumLAS']
            
            DT.loc[dt['index'].to_numpy(), ['AllocPrem']] = dt['AllocPrem'].to_numpy() #! important: this code make sure the index align 
        else: 
            DT = DT.merge(sumLAS, on='PolicyID')
            DT['AllocPrem'] *= DT['PolPrem'] / DT['sumLAS']
        
        return DT
        
    