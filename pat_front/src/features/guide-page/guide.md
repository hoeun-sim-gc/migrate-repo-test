<style>
.img_r {
    -webkit-filter: invert(1);
    filter: invert(1);
    },
hr{
    color: 'white'
}
</style>

## User's Guide

<hr class='img_r'/>

---

### **The Input Data**

[(download sample data)](samples/sample_data.zip)

The tool supports three types of input data
1. A policy list include the terms and premium, with an unique policy id for each policy
1. A location list with each location contains the policy it associated, TIV, ocucpancy type, etc.
1. Optionally a list of FAC treaties that apply to locations

There are three ways to input the data into the tool: 
1. RMS EDM (with optinally a Spider analysis in RDM): This will capture the most detailed information include the policy conditions, fac coverage, etc. This is the prefered way to input data to the tool.
1. Reference to a previous PAT analysis if the base data is the same and the only change is the rating type and parameters, default rating group, blending, etc.
1. User input files in CSV or Excel format:
    * **Policy**: A simple list of the policy terms with an identifier.
        * `PolicyID`: Identify the original policy.
        * `Limit`: Policy limit.
        * `Retention`: Policy Retention.
        * `Participation`: Policy participation 
        * `LossRatio`: Polcy loss ratio, can keep as "null" then when fill in with the  rating parameter, eg., 80%
        * `PolPrem`: Policy premium. 

    * **Location**: A list of locations with TIV and policy identifier.       
        * `LocID`: This column is used to match back the results with original input data. 
        * `PolicyID`: Identify the original policy.
        * `AOI` or `TIV`: TIV or AOI depend on the selected rating type.
        * `Stack`: Stack identifier when consider reinsurance. use LocID if you don't have any other input 
        * `RatingGroup`: In PSOLD case this is the rating group mapped from occupancy. Otherwise can be "null" and will set by the parameters
    
    * **PseudoPolicy**: Use this as an alternative to input policy and location seperatly. A PseudoPolicy is a record from joining the policy table with location table. A `PseudoPolicyID` is created from the join. Beside the columns in the above policy and location table, the following columns can be added in the input as well:
        * `PseudoPolicyID`: Record identifier. If this is for data correction, the column is required and have to be conssitent with orginal data. 
        * `ACCGRPID`: Optional
        * `PolRetainedLimit`: Policy retained limit
        * `Limit`: Policy limit.
        * `Retention`: Policy Retention.
        * `occupancy_scheme`: Optional  
        * `occupancy_code`: Optional
        * `TIV`: TIV or AOI depend on the selected rating type.
        * `Building`: Optional. Building value of the location
        * `Contents`: Optional. Contents value of the location
        * `BI`: Optional. BI value of the location
    
    * **Fac**: Optional. This input a list of Fac treaties apply to the policy location. If policy and location are input seperatly, this file have to include "`PolicyID`" and "`LocID`". If PseudoPolicy list is the input, then "`PseudoPolicyID`" have to be included.i
        * `FacKey`: Sequential number to identify each record. If this is for data correction, then FacKey is required and match the original data. Otherwise it is optional
        * `PseudoPolicyID`: or "`PolicyID`" and "`LocID`"
        * `FacLimit`: Fac limit
        * `FacAttachment`: Fac Attachment
        * `FacCeded`: FAC ceded %

You can zip all the user file(s) before input to the tool to speed up the file transfer to the server. 

[(download sample data)](samples/sample_data.zip)

### **Data Validation/Correction**

Once the tool start to calculate an analysis, it checks the data and make sure the input data is correct. It will flag all those data and depend on the user's choice of either stop and wait for a correction, or continue with those errorous entries removed. A user can download the data with error flags so decide how to fix the data, then load the data correction back to the tool for new calculation.

* Sometimes we don't want to change the input data and but instruct the tool how to handle the data error. We call those "validation rules", for example, Us the maximum AOI if a location have multiple calculated TIV due to the Fac treaty. For those rules user only need to check certain boxes in the UI to achieve the data correction.

* If a user decided to correct the data from the source, say, the EDM, the tool can expor the detail data with all error flags to help user identify and fix the issue, then reinport to the tool and create a new analysis.

* Sometime, however, a user may not be able to correct the data from the data source. For exampel, a user may not have access to update the EDM. in this case, the tool can still help to export the data with error flags. A user can then locate the the rows/column need to be corrected and make the correction. for those rows that is correct already, user can simply delete them. For columns that need to be corrected, make the change in a new column with "_revised" as the suffix to the column name. For example, make the change in column "**A**" by create a new column called "**A_revised**".

[(download sample data)](samples/sample_data.zip)       

### **The Rating Types**

The PAT tool support several kind of rating types
1. The PSOLD curves include 2016 and 2020 curves. PSOLD 2016 curve only support "Building+Contents+BI" and "Building+Contents" coverage type. PSOLD 2020 supports "Building Only" and "Contents Only."     
1. FLS curves include an entry for user defined
1. MB curves include an entry for user defined

When use PSOLD and if the rating group mapped from occupency is not available, we can use blending option by provide a list of blending weights. If only once curve is used, this is the same as setting a default rating group for missing/invalid groups. In fact, this blending option can apply to all locations if a user choose to.

### **How to Use the Tool**

The tool is web based so no installation is required. It is easy to use. 

1. When open the tool url in a browser, you will the list of jobs has been run before. You can only see the jobs you created, plus a list of "public" demo projects that visible to all users.   

<p align = "center">

![main](../../../public/images/pat_jobs.png "Main page with job list")
</p>
<p align = "center">
Fig.1 - Main page for job list<br />
(Can rerun, stop, and download from this page)
</p>


2. Click the "New Analysis" button on the left side bar then you can go to the "New Anlysis" page. From here you can select the input data, configure the rating type and parameters, then submit the job.     

<p align = "center">

![input](../../../public/images/pat_input.png "Select the input data")
</p>
<p align = "center">
Fig.2 - Input data<br />
(Can select to input data from Cat DB, reference job, or user defined)
</p>

<p align = "center">

![rating](../../../public/images/pat_rating.png "Select and configure rating type")
</p>
<p align = "center">
Fig.2 - Input data<br />
(Can select can configure rating type from PSOLD, FLS, and MB)
</p>

<p align = "center">

![submit](../../../public/images/pat_submit.png "Other configuration and submit the job")
</p>
<p align = "center">
Fig.2 - Input data<br />
(Final configuration and submit the job)
</p>

<br />
<br />

Once the job is submitted, it is sent to the backend server to calculate. You will stay in the same page and can make small modifications for the next job. You can submit multiple jobs in the same time and typically all will executed right away. We do set a maximum of execution (current is 5 in DEV server). The workers on the server will looking for unfinished jobs and start them. 

<br />
<br />
<br />
<br />

### Found bugs? Please send it to me. Thanks! 
$$f(x)  = \frac{1}{\sigma\sqrt{2\pi}}e^{-\frac{1}{2} (\frac{x-\mu}{\sigma})}$$  
😊

