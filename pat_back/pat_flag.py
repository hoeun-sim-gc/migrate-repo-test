import numpy as np
import pandas as pd
from enum import IntFlag

flag_descrs = {
        'FlagPolLocDupe' : "Duplicate policy/location records",
        'FlagPolNA' : "NonNumeric or negative field in policies" ,
        'FlagPolLimitParticipation' : "Algebraic inconsistency in policy profile (limit/participation)" ,
        'FlagPolParticipation' : "Participation exceeds 100%" ,
        'FlagFacOrphan' : "Orphan Facultative Record" ,
        'FlagFacNA' : "NonNumeric or negative field in fac table" ,
        'FlagLocIDDupe' : "AOI differences on location ID stack value",
        'FlagLocNA' : "NonNumeric or negative field in location table",
        'FlagLocRG' : "Rating Group is not in the range",
        
        'FlagFacOverexposed' : "Fac attachment/exposure exceeds policy exposure",
        'FlagCeded100' : "Amount ceded exceeds 100%"
    }
    #'NoteFacOverexposed1' : "Fac exposure exceeds policy exposure",

class PatFlag(IntFlag):
    FlagPolLocDupe = 0x00000001
    FlagPolNA = 0x00000002
    FlagPolLimitParticipation = 0x00000004
    FlagPolParticipation = 0x00000008
    FlagFacOrphan = 0x00000010
    FlagFacNA = 0x00000020
    FlagLocIDDupe = 0x00000040
    FlagLocNA = 0x00000080
    FlagLocRG = 0x00000100

    FlagFacOverexposed = 0x00001000
    FlagCeded100 =  0x00002000

    @classmethod
    def describe(cls, code):
        if not code:
            return ''

        lst = [(flag_descrs[n] if (f.value & code & 0x00FFFFFF) else 0) for n,f in cls.__members__.items()] # do not consider last two items
        lst = list(filter(None, lst))

        return ', '.join(lst)
        
    def set_flag(self, df, mask):
        if np.any(mask):
            df.loc[mask,['flag']] |= self.value

class ValidRule(IntFlag):
    ValidAoi = 0x00000001   # If multiple AOIs, use the highest
    ValidFac100 = 0x00000002   # Cap FAC to 100%

    ValidContinue = 0x40000000   # Continue with unhandled error (item removed)
