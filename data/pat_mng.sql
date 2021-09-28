CREATE SEQUENCE pat_analysis_id_seq  
    AS int  
    START WITH 100000000  
    INCREMENT BY 1 ;  

create table pat_job(
    job_id integer primary key,
    job_guid char(100),
    job_name varchar(100),
    receive_time varchar(30),
    update_time varchar(30),
    status varchar(100),
    user_name varchar(100),
    user_email varchar(100),
    parameters varchar(max)
)

create table pat_policy(
    job_id int,
    OriginalPolicyID int,
    PseudoPolicyID varchar(50),
    PolRetainedLimit float,
    PolLimit float,
    PolParticipation float,
    PolRetention float,
    PolPremium float
)

create table pat_location(
    job_id int,
    PseudoPolicyID varchar(50),
    AOI float,
    LocationIDStack varchar(20),
    RatingGroup int
)

create table pat_facultative(
    job_id int,
    PseudoPolicyID varchar(50),
    FacLimit float,
    FacAttachment float,
    FacCeded float,
    FacKey int
)

create table pat_premium(
    job_id int,
    Limit float,
    Retention float,
    Premium float, 
    Participation float, 
    AOI float,
    LocationIDStack varchar(20),
    RatingGroup int,
    OriginalPolicyID int,
    PseudoPolicyID varchar(50),
    PseudoLayerID int,
    PolLAS float,
    DedLAS float
)