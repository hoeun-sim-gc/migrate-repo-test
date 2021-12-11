CREATE SEQUENCE pat_analysis_id_seq  
    AS int  
    START WITH 100000000  
    INCREMENT BY 1;  

create table psold_mapping(
    OCCSCHEME varchar(20),
    OCCTYPE varchar(20),
    OCCDESC varchar(200),
    PSOLD_RG int,
    PSOLD_Description varchar(100),
    Effective varchar(15),
    Expiration varchar(15)
);

create table psold_weight0(
    RG int,
    OccupancyType varchar(50),
    PremiumWeight float,
    HPRMap varchar(10),
    HPRTable int
);

create table psold_weight(
    index int,
    AOI_gross int,
    AOI_gross int,
    PremiumWeight float,
    HPRMap varchar(10),
    HPRTable int
);

create table psold_aoi(
    AOI float
);

create table psold_gu_2016(
    COVG int,
    SUBGRP int,
    RG int,
    EG int,
    AOCC float,
    G1 float,
    G2 float,
    G3 float,
    G4 float,
    G5 float,
    G6 float,
    G7 float,
    G8 float,
    G9 float,
    G10 float,
    G11 float,
    OCC float,
    R1 float,
    R2 float,
    R3 float,
    R4 float,
    R5 float,
    R6 float,
    R7 float,
    R8 float,
    R9 float,
    R10 float,
    R11 float
);

create table pat_job(
    job_id integer primary key,
    job_guid char(100),
    job_name varchar(100),
    receive_time varchar(30),
    update_time varchar(30),
    status varchar(100),
    data_extracted int,
    user_name varchar(100),
    user_email varchar(100),
    parameters varchar(max)
);
create index idx_pat_job on pat_job (job_id asc);

create table pat_pseudo_policy(
    job_id int not null,
    data_type int, --0:used, 1: raw, 2: corrected 
    PseudoPolicyID varchar(50),
    ACCGRPID int,
    OriginalPolicyID int,
    PolRetainedLimit float,
    PolLimit float,
    PolParticipation float,
    PolRetention float,
    PolPremium float,
    LocationIDStack varchar(20),
    occupancy_scheme varchar(20), 
    occupancy_code varchar(20),
    Building float,
    Contents float,
    BI float,
    AOI float,
    RatingGroup int,
    flag int
);
create index idx_pat_pseudo_policy on pat_pseudo_policy (job_id asc);

create table pat_facultative(
    job_id int not null,
    data_type int, --0:used, 1: raw, 2: corrected 
    PseudoPolicyID varchar(50),
    FacLimit float,
    FacAttachment float,
    FacCeded float,
    FacKey int,
    flag int
);
create index idx_pat_facultative on pat_facultative (job_id asc);

create table pat_premium(
    job_id int,
    PseudoPolicyID varchar(50),
    PseudoLayerID int,
    Limit float,
    Retention float,
    Participation float, 
    Premium float, 
    PolLAS float,
    DedLAS float
);
create index idx_pat_premium on pat_premium (job_id asc);
