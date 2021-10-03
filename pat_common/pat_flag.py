import numpy as np
import pandas as pd
from enum import IntFlag

flag_descrs = {
        'FlagPolDupe' : "Duplicate policy records" ,
        'FlagPolNoLoc' : "Orphan Policy Record" ,
        'FlagPolNA' : "NonNumeric field in policies" ,
        'FlagPolNeg' : "Negative field in policies" ,
        'FlagPolLimitParticipation' : "Algebraic inconsistency in policy profile (limit/participation)" ,
        'FlagPolParticipation' : "Participation exceeds 100%" ,
        'FlagFacOrphan' : "Orphan Facultative Record" ,
        'FlagFacNA' : "NonNumeric field in fac table" ,
        'FlagFacNeg' : "Negative field in fac table" ,
        'FlagLocDupe' : "Duplicate location records" ,
        'FlagLocOrphan' : "Orphan Location Record" ,
        'FlagLocIDDupe' : "AOI differences on location ID stack value" ,
        'FlagLocNA' : "NonNumeric field in location table" ,
        'FlagLocNeg' : "Negative field in location table" ,
        'FlagLocRG' : "Rating Group is not in the range",
        
        'FlagFacOverexposed' : "Fac attachment/exposure exceeds policy exposure",
        'FlagCeded100' : "Amount ceded exceeds 100%"
    }
    #'NoteFacOverexposed1' : "Fac exposure exceeds policy exposure",

class PatFlag(IntFlag):
    FlagPolDupe = 1
    FlagPolNoLoc = 2
    FlagPolNA = 1 << 2
    FlagPolNeg = 1 << 3
    FlagPolLimitParticipation = 1 << 4
    FlagPolParticipation = 1 << 5
    FlagFacOrphan = 1 << 6
    FlagFacNA = 1 << 7
    FlagFacNeg = 1 << 8
    FlagLocDupe = 1 << 9
    FlagLocOrphan = 1 << 10
    FlagLocIDDupe = 1 << 11
    FlagLocNA = 1 << 12
    FlagLocNeg = 1 << 13
    FlagLocRG = 1 << 14

    FlagFacOverexposed = 1 << 15
    FlagCeded100 = 1 << 16

    @classmethod
    def describe(cls, code):
        lst = [(flag_descrs[n] if f.value & code else None) for n,f in cls.__members__.items()]
        lst = list(filter(None, lst))

        return ', '.join(lst)
        
    def set_flag(self, df, mask):
        if np.any(mask):
            df.loc[mask,['status']] += self.value

    
