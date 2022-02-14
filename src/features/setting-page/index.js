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
          control={<Switch isDark={isDark} onChange={toggleChecked} color="primary"/>}
          label="Browse in dark mode"
        />
      </FormGroup>
    </div>
  );
}
