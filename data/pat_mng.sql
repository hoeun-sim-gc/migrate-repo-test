create table pat_job(
    job_id varchar(100), -- in Datalake this field will be used to partion the data, so we don't need to create integer id 
    job_name varchar(100),
    receive_time datetime,
    update_time datetime,
    status varchar(100),
    user_name varchar(100),
    user_email varchar(100)
)

create table pat_result(
    job_id varchar(100),
    Limit float,
    Retention float,
    Prem float, 
    Participation float, 
    LossRatio float, 
    AOI float,
    LocationIDStack varchar(20),
    RatingGroup int,
    OriginalPolicyID int,
    PseudoPolicyID varchar(50),
    PseudoLayerID int,
    PolLAS float,
    DedLAS float
)