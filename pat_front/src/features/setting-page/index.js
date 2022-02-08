import React, { useState } from "react";
import Switch from '@material-ui/core/Switch';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import { Form, FormGroup, FormControl } from "react-bootstrap";

import "./index.css";

export default function Settings(props) {
  const [isDark, setIsDark] = React.useState(props.theme==='dark');

  React.useEffect(() => {
  }, []);

  
  React.useEffect(()=>{
    const mode= isDark?'dark':'light'
    props.ChangeTheme(mode);
    localStorage.setItem("prefer_theme",mode);
  },[isDark, props]);

  const toggleChecked = () => {
    setIsDark(!isDark);
  };

  return (
    <div className="Settings">
      <FormGroup>
        <FormControlLabel
          control={<Switch checked={isDark} onChange={toggleChecked} />}
          label="Browse in dark mode"
        />
      </FormGroup>
      {/* <Form>
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
      </Form> */}
    </div>
  );
}
