import React, { useState } from "react";
import Switch from '@material-ui/core/Switch';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import { Form, Button, FormGroup, FormControl } from "react-bootstrap";

import "./index.css";

export default function Settings(props) {
  const [checked, setChecked] = React.useState(props.theme==='dark');

  const [newPsize, setNewPsize]=useState(20);

  React.useEffect(() => {
    let ps = 20;
    try
    {
      var s = localStorage.getItem('job_page_size');
      ps = parseInt(s);
    }
    catch
    {
      ps=20;
    }

    if (ps && ps > 0) setNewPsize(ps)
  }, []);

  
  React.useEffect(()=>{
    const mode= checked?'dark':'light'
    props.ChangeTheme(mode);
    localStorage.setItem("prefer_theme",mode);
  },[checked, props]);

  const toggleChecked = () => {
    setChecked(!checked);
  };

  return (
    <div className="Settings">
      <FormGroup>
        <FormControlLabel
          control={<Switch checked={checked} onChange={toggleChecked} />}
          label="Browse in dark mode"
        />
      </FormGroup>
      <Form>
        <div className="row align-items-end" >
          <div className="col">
            <FormGroup>
              <label className="float-left">Job List Number Per Page:</label>
              <FormControl 
                  value={newPsize}
                  placeholder="Enter page size"
                  onChange={
                    e => {
                      setNewPsize(e.target.value);
                      localStorage.setItem("job_page_size",e.target.value );
                    }
                  } />
            </FormGroup>
          </div>
          <div className="col" />
          <div className="col" />
        </div>
      </Form>
    </div>
  );
}
