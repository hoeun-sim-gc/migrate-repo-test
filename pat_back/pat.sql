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

create table psold_weight(
    RG int,
    OccupancyType varchar(50),
    PremiumWeight float,
    HPRMap varchar(10),
    HPRTable int
);

create table psold_hpr_weight (
    Limit float,
    Weight float
);

create table psold_aoi(
    AOI float
);

create table rating_curves(
    ID int not null, ---2016, 2020, or FLS, MB curve id
    Curve varchar(20),
    COVG int,
    SUBGRP int,
    RG int,
    EG int,
    OCC float,
    W1 float,
    W2 float,
    W3 float,
    W4 float,
    W5 float,
    W6 float,
    W7 float,
    W8 float,
    W9 float,
    W10 float,
    W11 float
);

create table fls_curves(
	ID int primary key,
	name varchar(255),
	mu float,
	w float,
	tau float,
	theta float,
	beta float,
	cap money,
	uTgammaMean float,
	limMean float
);

create table mb_curves(
	ID int primary key,
	name nvarchar(255),
	c float,
	b float,
	g float,
	cap float
);

create table pat_job(
    job_id integer primary key,
    job_guid char(100),
    job_name varchar(100),
    receive_time varchar(30),
    start_time varchar(30),
    finish_time varchar(30),
    status varchar(100),
    data_extracted int,
    user_name varchar(100),
    user_email varchar(100),
    parameters varchar(max)
);

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
    LossRatio float,
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
