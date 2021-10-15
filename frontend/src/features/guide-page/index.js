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
          Content sdfl; sdlf fsd
        </div>
        <div class="pat_bottom_row">
            Content sdfl; sdlf fsd l; lsdfl slfsdfdffd dfio i 
        </div>
      </div>
      <div class="pat_right_col">
        <h2>Welcome to Premium Allocation Tool</h2>
        <br />
        <h5>Introduction:</h5>
        <p>blah blah.ds ,mssfjlskfsdjl sdfj lsjfk lsf shfk kdsfhk sdkhkds fksdhf khsf kshf ksfwef..</p>
        <p>blah blah.ds ,mssfjlskfsdjl sdfj lsjfk lsf shfk kdsfhk sdkhkds fksdhf khsf kshf ksfwef..</p>
        <p>blah blah.ds ,mssfjlskfsdjl sdfj lsjfk lsf shfk kdsfhk sdkhkds fksdhf khsf kshf ksfwef..</p>
      </div>
    </div>
  );
};
