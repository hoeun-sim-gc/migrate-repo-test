from datetime import datetime
import numpy as np
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

class PAT_FLAG(IntFlag):
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

class VALIDATE_RULE(IntFlag):
    ValidAoi = 0x00000001   # If multiple AOIs, use the highest
    ValidFac100 = 0x00000002   # Cap FAC to 100%

    ValidContinue = 0x40000000   # Continue with unhandled error (item removed)

class DATA_SOURCE_TYPE(IntFlag):
    Reference_Job = 1,
    User_Upload = 2,
    Cat_Data = 3

class COVERAGE_TYPE(IntFlag):
    Building = 1,
    Contents = 2,
    BI = 4,

    Building_Contents = 3, 
    Building_Contents_BI = 7,
    Building_Only = 1,
    Contents_Only = 2 


class PERIL_SUBGROUP(IntFlag):
    Fire = 1,
    Wind = 2, 
    Special_Causes = 3,
    All_Perils = 4

class DEDDUCT_TYPE(IntFlag):
    Retains_Limit = 1,
    Erodes_Limit = 2

class RATING_TYPE(IntFlag):
    PSOLD = 1,
    FLS = 2
    MB = 3

class PSOLD_PERSP(IntFlag):
    Gross = 1,
    Net = 2

