import numpy as np
import pandas as pd
from enum import IntFlag

flag_descrs = {
        'FlagPolDupe' : "Duplicate policy records" ,
        'FlagPolOrphan' : "Orphan Policy Record" ,
        'FlagPolNA' : "NonNumeric or negative field in policies" ,
        'FlagPolLimitParticipation' : "Algebraic inconsistency in policy profile (limit/participation)" ,
        'FlagPolParticipation' : "Participation exceeds 100%" ,
        'FlagFacOrphan' : "Orphan Facultative Record" ,
        'FlagFacNA' : "NonNumeric or negative field in fac table" ,
        'FlagLocDupe' : "Duplicate location records" ,
        'FlagLocOrphan' : "Orphan Location Record" ,
        'FlagLocIDDupe' : "AOI differences on location ID stack value",
        'FlagLocNA' : "NonNumeric or negative field in location table",
        'FlagLocRG' : "Rating Group is not in the range",
        
        'FlagFacOverexposed' : "Fac attachment/exposure exceeds policy exposure",
        'FlagCeded100' : "Amount ceded exceeds 100%"
    }
    #'NoteFacOverexposed1' : "Fac exposure exceeds policy exposure",

class PatFlag(IntFlag):
    FlagPolDupe = 0x00000001
    FlagPolOrphan = 0x00000002
    FlagPolNA = 0x00000004
    FlagPolLimitParticipation = 0x00000008
    FlagPolParticipation = 0x00000010
    FlagFacOrphan = 0x00000020
    FlagFacNA = 0x00000040
    FlagLocDupe = 0x00000080
    FlagLocOrphan = 0x00000100
    FlagLocIDDupe = 0x00000200
    FlagLocNA = 0x00000400
    FlagLocRG = 0x00000800

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
