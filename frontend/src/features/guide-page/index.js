import React, { useState, useContext, useRef } from 'react';
import { makeStyles, useTheme } from '@material-ui/core/styles';
import {
  Grid, Card, CardContent,
  InputLabel, FormControl, Select, Divider,
  Typography,
  MenuList, MenuItem
} from '@material-ui/core';
import ReactSpeedometer from "react-d3-speedometer"

import { PulseLoader } from "react-spinners";
import 'bootstrap/dist/css/bootstrap.min.css';
import 'react-bootstrap-table-next/dist/react-bootstrap-table2.min.css';

import { UserContext } from "../../app/user-context";

import "./index.css";

export default function GuidePage(props) {
  const theme = useTheme();

  const [user,] = useContext(UserContext);

  const items = [...Array(100)].map((val, i) => `Item ${i}`);

  return (
    <div class="pat_container">
      <div class="pat_left_col pat_container1">
        <div class="pat_top_row">
          <h1>Welcome to Premium Allocation Tool</h1>
          <br />

          <h3>Business Background and Objectives</h3>
          <p>
            Exposure rating is a process of using the information about the exposure of a risk or portfolio to determine a rate to charge. At Guy Carpenter, exposure rating is an important part of pricing property excess-of-loss reinsurance. The input for the Per Risk Excess-of-Loss exposure rating analysis has traditionally been a policy limits profile. However, policy limits profiles do not capture the level of data necessary to accurately exposure rate multi-location policies. Standard methodology using a policy limits profile applies the policy limit to each location. However, the policy limit is typically the value of the largest risk, while in reality, most policies consist of locations with a range of values, some much smaller than the policy limit. By assuming all risks on a policy are as large as the policy limit, traditional methods of exposure rating have a tendency to overstate excess layer losses.
          </p>
          <p>
            To prevent this problem, it is necessary to use location-level profiles for per risk excess-of-loss exposure rating. This introduces an additional difficulty, as premium is typically assigned to multi-location policies at the policy level, not at the location level.
          </p>
          <p>
            In 2003, Guy Carpenter built a tool to allocate policy-level property premium to the location level for policies with multiple insured locations. We have a number of clients that depend on this process to more accurately complete the exposure rating analysis (e.g. RSUI, ACE, OneBeacon, Commonwealth, Chubb, etc). This tool was programmed into both Excel and Access. Unfortunately, the current Excel-based tool is very slow and labor intensive and the current Access tool has many flaws due to its age, non-professional programming, and original creation for one specific client.
          </p>
          <p>
            These existing tools can be used as proto-types for rebuilding the tool, but the platform will likely need to be something other than Excel or Access, due to the size of the files we are often dealing with. Rebuilding the premium allocation tool will result in a more efficient and more technically sound tool, and it would allow us to incorporate additional functionality, such as identifying data quality issues within a company’s portfolio.
          </p>
          <p>
            Across all clients, this has the potential to save hundreds of hours per year of analytics work.
            In addition, accurate premium allocation will avoid over-estimation of exposure and therefore will reduce reinsurance costs for our clients. Redeveloping this tool is a necessary step to continue our competitive advantage in this area of analysis.
          </p>
          <p>
            In addition, eventually we would like this tool to completely replace the existing Excel-based property exposure model. This would save even more hours per year of analytical work, and could eliminate some of the possibilities for errors in this process.
          </p>
        </div>
        <div class="pat_bottom_row">
          <h3>Table of Contents</h3>
          <ul>
            <li><a href="javascript:;">Objective</a></li>
            <li>Prepare EDM</li>
            <li>Add a Spider analysis</li>
            <li>Set up allocation analysis</li>
            <li>Validate data and make corrections</li>
            <li>Export the allocated premium, or push back to EDM</li>
          </ul>
        </div>
      </div>
      <div class="pat_right_col">
        <h3>OBJECTIVE</h3>
        <p>
          The general goals for this application would be:
        </p>
        <ol>
          <li>Pull policy- and location-level data from an RMS EDM or import exposure data from another file format such as Excel, text, etc. If the data is imported from a file other than an EDM, we can require that it be formatted in a standard way.
          </li>
          <li>
            Create some diagnostic reports to help identify some common data quality issues such as currency issues, unusually large or small policy limits or TIVs, bulk coding, or overlapping policy layers*. This could be done by building standard profiles by policy limit, TIV, occupancy type, etc. *Note that the current Access-based premium allocation tool does identify overlapping policy layers, and it will not allow the user to continue with the premium allocation process until all overlapping policies are ‘fixed.’ However, there are cases when ‘overlapping’ policies are legitimate so we do not want there to be a requirement that all policies be non-overlapping.
          </li>
          <li>Incorporate any inuring policy-level reinsurance terms, such as facultative or surplus share reinsurance. During this process, an individual policy may need to be ‘split’ into multiple layers in order to accurately reflect the inuring protection of the reinsurance. See figure below as an example. The original policy was 100% of $70M excess of $10M, but because of the inuring reinsurance structure, it would need to be split into five different layers to accurately reflect the inuring reinsurance.
          </li>
          <img alt='layers' src={require('./pat_layer_stack.png')} />
          <li>Calculate the limited average severity (LAS) for each location/layer combination. This can be done using a choice of actuarial severity curves (PSOLD, First Loss Scales, user defined, etc.).
          </li>
          <li>Allocate the net policy premium (net of any premium ceded to inuring reinsurance by policy) to the location level using the same distribution of LAS (the LAS for an individual location/layer combo compared to the sum of all LAS for all location/layer combos).
          </li>
          <li>Create location-level profiles using the location-level premium developed in the previous steps.
          </li>
          <li>Run property exposure model against user-defined per risk reinsurance layers.
          </li>
          <li>Create CDFs for use for various per risk reinsurance layers for use in Metarisk.
          </li>
        </ol>
      </div>
    </div>
  );
};
