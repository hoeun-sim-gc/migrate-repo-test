from datetime import datetime
import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame


class Psold:
    def __init__(self, df_psold, aoi_split, trendF, trendFrom):
        """Class to represent PSOLD curve utilities
            df_psold: ['RG', 'EG', 'OCC', 'W1', 'W2', 'W...']
            df_occ_wts: ['RG', 'Type', 'HPRMap'], ID = RG
            aoi_split: Array with lower in i and upper in i + 1
        """

        # ? Add some data checking?

        self.treeand_fctr = trendF
        self.trend_from = trendFrom
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

    def __blending(self, wgts, adCvg):
        """Fully vectorized numpy version. It takes the occupancy weight (OW) vector 
        and blends the component sets of 11 parameters by their proportion of the OW, 
        using the ISO credibility counts and the LAS's so that the result reflects the mix.

        Assume wgts is normalized
        """
        x = (self.aoi_split[1:] * (adCvg + 1))[:, np.newaxis]
        AOI_LAS = self.melas(x, self.psold_a, self.mu)

        tblidx = list(*np.where(wgts > 0))
        AOI_Prem = np.sum(AOI_LAS[:, tblidx] *
                          self.psold_c[:, tblidx], axis=1)
        AOI_CCount = wgts * (AOI_Prem[:, np.newaxis] / AOI_LAS)

        CustomCounts = np.sum(AOI_CCount, axis=1)
        CustomWeights = np.sum(
            self.psold_a * AOI_CCount[:, :, np.newaxis], axis=1) / CustomCounts[:, np.newaxis]

        return (CustomWeights, CustomCounts)
    
    def get_blending_weights(self, df_wts:DataFrame, adCvg:np.number, hpr:bool = False):      
        """ 
            df_wts: ['OccupancyType', 'PremiumWeight', 'HPRMap'], with 'RG' as index.
            If hpr is true, we need additional column 'HPRTable'
        """

        df = df_wts.sort_index()
        if hpr:
            df = df.groupby('HPRTable').agg({'PremiumWeight':sum}).reindex(range(1, self.max_rg+1), fill_value=0)
        
        wgts = df.PremiumWeight.astype('float').to_numpy()
        wgts /= np.sum(wgts)
        return self.__blending(wgts, adCvg)

    def melas(self, x, w, mu):
        """Fully vectorized numpy verion of MEALS to calculate limited average severity of the mixed exponential distribution. 
            The leading dimensions of x and will be used as batch
            x: loss (..., n )
            w: weights (..., n, m), already normalized by i
            nu: (m, ) Adjusted for currency and trend
        """

        return (w * (1 - np.exp(-x[..., np.newaxis] @ (1/mu[np.newaxis, :])))) @ mu

    def pat(self, DT:DataFrame, df_wts:DataFrame, df_hpr:DataFrame = None, inplace=True, **kwargs) -> DataFrame:
        """
            DT: ['polID', 'locID', 'limit', 'polDed', 'prem', 'LR', 'participate', 'rawTIV', 'stack', 'rtG'] 
            df_wts: ['OccupancyType', 'PremiumWeight', 'HPRMap'], with 'RG' as index.
            df_hpr: ['Limit', 'Weight']
        """

        para = {
            'dedType' : 1,
            'curAdj' : 1,
            'avAccDt' : datetime(2021,1,1),
            'adCvg' : 2
        }
        para.update(kwargs)

        trF = np.power(self.treeand_fctr, (para['avAccDt'] - self.trend_from).days / 365.25)
        adjf = para['curAdj'] * trF
        acp1 = para['adCvg'] + 1 

        if inplace: 
            DT.fillna({'polDed':0,'participate':1}, inplace=True)
        else:
            DT = DT.fillna({'polDed':0,'participate':1})

        DT['polLmt'] = (DT.limit + (DT.polDed if para['dedType'] == 1 else 0)).fillna(0)
        DT['policy'] = DT.polDed + np.maximum(DT.polLmt - DT.polDed, 0) * (DT['stack'].isna() * (acp1-1) + 1) 
        DT['TIV'] = DT.rawTIV.fillna(0)
        DT['effLmt'] = np.minimum(DT.TIV, DT.polLmt) * acp1 
        DT['AOIr'] = np.searchsorted(self.aoi_split, DT.TIV / adjf, side='left') 

        wgt = pd.DataFrame(data = np.zeros((20, 11)), columns=[f'W{i}' for i in range (1, 12)])
        mask_rg  = DT.rtG.isna().to_numpy() #! important: Have to convert to numpy, otherwise can mess up if DT is not in natrual order  
        if any(mask_rg):
            wt, _ = self.get_blending_weights(df_wts, para['adCvg']) 
            wgt.loc[mask_rg] = wt[DT.loc[mask_rg,'AOIr'] -1 ,:]
        wgt.loc[~mask_rg] = self.psold_a[DT.loc[~mask_rg,'AOIr'] - 1, DT.loc[~mask_rg,'rtG'].astype('int') -1 ,:]
        wgt=wgt.to_numpy()

        x = np.vstack((np.minimum(DT.policy,DT.effLmt).to_numpy(),
                np.minimum(DT.polDed,DT.effLmt).to_numpy()))
        DT[['polLAS','dedLAS']] = self.melas(x,wgt,self.mu*adjf).T
        DT[['guLimLAS','guDedLAS']] = DT[['polLAS','dedLAS']]

        if df_hpr is not None:
            wgt = pd.DataFrame(data = np.zeros((20, 11)), columns=[f'W{i}' for i in range (1, 12)])
            if any(mask_rg):
                wt, _ = self.get_blending_weights(df_wts, para['adCvg'], True) 
                wgt.loc[mask_rg] = wt[DT.loc[mask_rg,'AOIr'] -1 ,:]
            wgt.loc[~mask_rg] = self.psold_a[DT.loc[~mask_rg,'AOIr'] - 1, df_wts.HPRTable[DT.loc[~mask_rg,'rtG'].astype('int')] - 1 ,:]
            wgt=wgt.to_numpy()

            DT[['polLASH','dedLASH']] =self.melas(x,wgt,self.mu*adjf).T
            DT['HPRWeight'] = np.interp(DT.TIV / adjf, df_hpr.Limit, df_hpr.Weight ) 
            DT['guLimLAS'] = DT.HPRWeight * DT.polLASH + (1 - DT.HPRWeight) * DT.polLAS
            DT['guDedLAS'] = DT.HPRWeight * DT.dedLASH + (1 - DT.HPRWeight) * DT.dedLAS
        
        DT['allocPrem'] = (DT.guLimLAS-DT.guDedLAS)*DT.LR
        
        sumLAS = DT.groupby('polID').agg({'allocPrem':'sum'}).rename(columns={'allocPrem':'sumLAS'})
        if inplace:
            dt = DT[['polID','prem','allocPrem']].reset_index()
            dt = dt.merge(sumLAS, on='polID')
            dt['allocPrem'] *= dt['prem'] / dt['sumLAS']
            
            DT.loc[dt['index'].to_numpy(), ['allocPrem']] = dt['allocPrem'].to_numpy() #! important: this code make sure the index align 
        else: 
            DT = DT.merge(sumLAS, on='polID')
            DT['allocPrem'] *= DT['prem'] / DT['sumLAS']
        
        return DT
        
    @classmethod
    def sample_pat(cls, hpr= False, **kwargs):

        """This is a sample code to create a PSOLD model and perform premium allocation. In production tool 
            we need to make the change for accessing the base data and policies
        """
        para = {
            'curveType' : 'gross',
            'covg' : 4,
            'subgrp' : 1, 
            'adCvg' : 2,
            'trendFctr' : 1.035,
            'trendFromDt' : datetime(2015,12,31)
        }
        para.update(kwargs)

        # Import basic curve data
        aoi_split = np.loadtxt(r"C:\_Working\PAT_20201019\debug\AOI_R.csv")
        df_psold = pd.read_csv("C:\_Working\PAT_20201019\debug\ISOt_2016.csv")

        col_ct = 'AOCC' if para['curveType'] == 'gross' else 'OCC'
        col_w = 'G' if para['curveType'] == 'gross' else 'R'
        col_wt = [f'{col_w}{i}' for i in range(1, 12)]
        df_psold = df_psold[(df_psold.COVG == para['covg']) & (df_psold.SUBGRP == para['subgrp'])][
            list(('RG','EG',col_ct,*col_wt))]
        df_psold.columns=['RG', 'EG', 'OCC', *[f'W{i}' for i in range(1, 12)]]

        # Now create the PSOLD model
        psold  = Psold(df_psold, aoi_split, para['trendFctr'], para['trendFromDt'])

        # inport policy and blending data and perform the premium allocation 
        df_wts = pd.read_csv(r"C:\_Working\PAT_20201019\debug\psold_weight.csv")
        DT = pd.read_csv("C:\_Working\PAT_20201019\debug\dt0.csv").rename(columns={'SIRD':'polDed'})
        df_hpr = pd.read_csv("C:\_Working\PAT_20201019\debug\HPRBlend.csv") if hpr else None

        # DT = DT.sort_values('polDed')#.reset_index(drop=True)
        return psold.pat(DT, df_wts, df_hpr, inplace=True)





