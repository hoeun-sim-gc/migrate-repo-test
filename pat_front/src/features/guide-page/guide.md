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

### **Input Data**

[(download sample data)](samples/sample_data.zip)

The tool supports three types of input data:
1. A policy list that includes the terms and premium, with an unique policy id for each policy
1. A location list with each location containing the associated policy, TIV, ocucpancy type, etc.
1. An optional list of FAC treaties that apply to locations

There are three ways to input the data into the tool: 
1. RMS EDM (with optinally a Spider analysis in RDM): This will capture the most detailed information, including the policy conditions, fac coverage, etc. This is the prefered way to input data into the tool.
1. Reference to a previous PAT analysis if the base data is the same and the only change is the rating type and parameters, default rating group, blending, etc.
1. Files in CSV or Excel format:
    * **Policy**: A simple list of the policy terms with an identifier
        * `PolicyID`: Original policy identifier
        * `Limit`: Policy limit
        * `Retention`: Policy retention
        * `Participation`: Policy participation 
        * `LossRatio`: Policy loss ratio; can keep as "null", then update with the  rating parameter, eg., 80%
        * `PolPrem`: Policy premium 

    * **Location**: A list of locations with TIV and policy identifier       
        * `LocID`: This column is used to match back the results with original input data 
        * `PolicyID`: Original policy identifier
        * `AOI` or `TIV`: TIV or AOI depends on the selected rating type
        * `Stack`: Stack identifier when considering reinsurance. Use LocID if you don't have any other input. 
        * `RatingGroup`: In the PSOLD case, this is the rating group mapped from occupancy. Otherwise, it can be "null" and set by the parameters.
    
    * **PseudoPolicy**: Use this as an alternative to input policy and location separatly. A PseudoPolicy is a record from joining the policy table with the location table. A `PseudoPolicyID` is created from the join. Besides the columns in the above policy and location table, the following columns can be added in the input as well:
        * `PseudoPolicyID`: Record identifier. If this is for data correction, the column is required and have to be consistent with orginal data. 
        * `ACCGRPID`: Optional
        * `PolRetainedLimit`: Policy retained limit
        * `Limit`: Policy limit
        * `Retention`: Policy Retention
        * `occupancy_scheme`: Optional  
        * `occupancy_code`: Optional
        * `TIV`: TIV or AOI depend on the selected rating type
        * `Building`: Optional. Building value of the location
        * `Contents`: Optional. Contents value of the location
        * `BI`: Optional. BI value of the location
    
    * **Fac**: Optional. A list of Fac treaties applied to the policy location. If policy and location are inputted seperatly, this file will need to include "`PolicyID`" and "`LocID`". If PseudoPolicy list is the input, then "`PseudoPolicyID`" have to be included.
        * `FacKey`: Sequential number to identify each record. If this is for data correction, then FacKey is required and must match the original data; otherwise, it is optional.
        * `PseudoPolicyID`: or "`PolicyID`" and "`LocID`"
        * `FacLimit`: Fac limit
        * `FacAttachment`: Fac Attachment
        * `FacCeded`: FAC ceded %

To speed up the file transfer to the server, you can zip all file(s) before inputting them to tool. 

[(download sample data)](samples/sample_data.zip)

### **Data Validation/Correction**

Once the tool starts and before it calculates an analysis, it checks the data to ensure that the input data is correct. It will flag all incorrect data, and depending on the user's choice, it will either 1) stop and wait for a correction, or 2) continue with those erroneous entries removed. A user can download the data with error flags, fix the data, then load the corrected data back to the tool for a new calculation.

* If we sometimes don't want to change the input data, we could instruct the tool on how to handle the data errors; we call these "validation rules". For example, Us the maximum AOI if a location has multiple calculated TIVs due to the Fac treaty. For these rules, the user only needs to check certain boxes in the UI to achieve the data correction.

* If a user decides to correct the data from the source, say, the EDM, the tool can export the detailed data with all error flags to help the user identify and fix the issue, then re-import to the tool and create a new analysis.

* Sometimes, however, a user may be unable to correct the data from the data source. For example, the user may not have access to the updated the EDM. In this case, the tool can still help by exporting the data with error flags. The user can then locate the the rows/columns needed to be corrected and make the correction. For rows that are already correct, the user can simply delete them. For columns that need correction, the user can make the change in a new column with "_revised" suffixed to the column name. For example, make the change in column "**A**" by creating a new column called "**A_revised**".

[(download sample data)](samples/sample_data.zip)       

### **Rating Types**

The PAT tool supports several kinds of rating types:
1. The PSOLD curves include 2016 and 2020 curves. PSOLD 2016 curve only supports "Building+Contents+BI" and "Building+Contents" coverage types. PSOLD 2020 supports "Building Only" and "Contents Only."     
1. FLS curves include an entry for user defined
1. MB curves include an entry for user defined

When using the PSOLD, if the rating group mapped from occupency is not available, we can use the blending option by providing a list of blending weights. If the curve is used only once, this is the same as setting a default rating group for missing/invalid groups. In fact, this blending option can apply to all locations if the user chooses.

### **How to Use the Tool**

The tool is web-based, so no installation is required; it is easy to use. 

1. When opening the tool's URL in a browser, you will see the list of previous job runs. You can only see the jobs you've created, plus a list of "public" demo projects, visible to all users.   

<p align = "center">

![main](../../../public/images/pat_jobs.png "Main page with job list")
</p>
<p align = "center">
Fig.1 - Main page for job list<br />
(Can rerun, stop, and download from this page)
</p>


2. Clicking the "New Analysis" button on the left side bar will direct you to the "New Anlysis" page. From here, you can select the input data, configure the rating type and parameters, then submit the job.     

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
(Can select to configure rating type from PSOLD, FLS, and MB)
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

Once the job is submitted, it is sent to the backend server to calculate. You will stay on the same page and can make small modifications for the next job. You can submit multiple jobs at the same time and, typically, all will be executed right away. We set a maximum of executions (currently, is 5 in DEV server). The workers on the server will look for unfinished jobs and start them. 

<br />
<br />
<br />
<br />

 
$$f(x)  = \frac{1}{\sigma\sqrt{2\pi}}e^{-\frac{1}{2} (\frac{x-\mu}{\sigma})}$$  
😊