import React, { useState, useEffect } from 'react';
import { useTheme } from '@material-ui/core/styles';
import SplitPane from "react-split-pane";
import Markdown from 'markdown-to-jsx';

import 'bootstrap/dist/css/bootstrap.min.css';
import 'react-bootstrap-table-next/dist/react-bootstrap-table2.min.css';

import Link from '@mui/material/Link';

import "./index.css";

export default function GuidePage(props) {
  const theme = useTheme();

  const [userGuide, setUserGuide] = useState('');
  
  const th  = localStorage.getItem("prefer_theme");
  const re_img  = RegExp(`\\(.*\\/public\\/images\/`, 'g');
  const re_img1  = RegExp(`".*\\/public\\/images\/`, 'g');
  const re_F = RegExp(`\\$\\$(.* ?)\\$\\$`, 'g');
  const re_f = RegExp(`\\$(.* ?)\\$`, 'g');
  
  useEffect(() => {
    import("./guide.md")
          .then(res => {
              fetch(res.default)
                  .then(res => res.text())
                  .then(res => 
                    {
                      var rev = th === 'light'?'':'math_r'
                      res = res.replace(re_img, '(images/')
                        .replace(re_img1, '"images/')
                      res = res.replace(re_F, 
                        `<div class='math_d'><img src='https://latex.codecogs.com/png.image?$1' class='`+rev+`'/></div>`)
                        .replace(re_f, 
                          `<img src='https://latex.codecogs.com/png.image?$1' class='`+ rev +` math_i' />`)
                        .replaceAll("&#36;", "$")

                      setUserGuide(res);
                    })
                  .catch(err => console.log(err));
          })
          .catch(err => console.log(err));
  }, []);

  return (
    <div>
      <SplitPane split="vertical" minSize={'10%'} defaultSize={'35%'} maxSize={'80%'} style={{position:'static'}} >
        <div class="pane_cont left_col">
          <h1>Premium Allocation Tool</h1>
          <p></p>
          <p>
            Premium Allocation Tool is to allocate policy-level property premium to the 
            location level for policies with multiple insured locations. The original tool 
            was build in 2003 and before this new version it is written in a combination of R 
            and VBA, with Excel as the frontend interface. This new veriosn has rewrite 
            from the very bottom and have solved a few long existing issue:  
          </p>
         
         <h3>New Features</h3>
          <ul>
            <li>Rebuild the tool as a web application. No installation is needed</li>
            <li>No more dependeny on Excel and R. The process is much more stable</li>
            <li>Support all the rating curves that suppported in GC PropExpo Rating Tool,
              Kepp the calculate algorithm consistent between these two tools. </li>
            <li>Rewrite the calculate core with fully vectorized algorithm to greatly speed up the calculation</li>
            <li>Save data of the whole process in database for easy checking and data reuse</li>
          </ul>
        </div>
        <div class="pane_cont right_col">
          <Markdown>{userGuide}</Markdown>
        </div>
      </SplitPane>
    </div>
  );
};
