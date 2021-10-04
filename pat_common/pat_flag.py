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
    FlagPolDupe = 0x00000001
    FlagPolNoLoc = 0x00000002
    FlagPolNA = 0x00000004
    FlagPolNeg = 0x00000008
    FlagPolLimitParticipation = 0x00000010
    FlagPolParticipation = 0x00000020
    FlagFacOrphan = 0x00000040
    FlagFacNA = 0x00000080
    FlagFacNeg = 0x00000100
    FlagLocDupe = 0x00000200
    FlagLocOrphan = 0x00000400
    FlagLocIDDupe = 0x00000800
    FlagLocNA = 0x00001000
    FlagLocNeg = 0x00002000
    FlagLocRG = 0x00004000

    FlagFacOverexposed = 0x00008000
    FlagCeded100 =  0x00010000

    # not error status 
    FlagCorrected =  0x10000000
    FlagActive =  0x20000000

    @classmethod
    def describe(cls, code):
        lst = [(flag_descrs[n] if (f.value & code & 0x00FFFFFF)  else None) for n,f in cls.__members__.items()] # do not consider last two items
        lst = list(filter(None, lst))

        return ', '.join(lst)
        
    def set_flag(self, df, mask):
        if np.any(mask):
            df.loc[mask,['status']] |= self.value

    
