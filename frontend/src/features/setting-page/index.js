import React, { useState, useContext } from "react";
import Switch from '@material-ui/core/Switch';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import { Form, Button, FormGroup, FormControl } from "react-bootstrap";

import "./index.css";
import { Grid } from "@material-ui/core";

export default function Settings(props) {
  const [checked, setChecked] = React.useState(props.theme==='dark');

  const [newServer, setNewServer]=useState('');
  
  React.useEffect(()=>{
    const mode= checked?'dark':'light'
    props.ChangeTheme(mode);
    localStorage.setItem("prefer_theme",mode);
  },[checked, props]);

  const toggleChecked = () => {
    setChecked(!checked);
  };

  function handleSubmit(event) {
    event.preventDefault();

    if(newServer.length > 0) {
      let lst= [];
      let js= localStorage.getItem('Server_List')
      if (js) lst = JSON.parse(js);
      lst.push(newServer.toUpperCase())

      lst = [...new Set(lst)].sort();
      localStorage.setItem("Server_List", JSON.stringify(lst));
    }
  }

  return (
    <div className="Settings">
      <FormGroup>
        <FormControlLabel
          control={<Switch checked={checked} onChange={toggleChecked} />}
          label="Browse in dark mode"
        />
      </FormGroup>
      <Form onSubmit={handleSubmit}>
        <div className="row align-items-end" >
          <div className="col">
            <FormGroup>
              <label className="float-left">Add Server:</label>
              <FormControl
                  value={newServer}
                  placeholder="Enter Server Name"
                  onChange={e => setNewServer(e.target.value)} />
            </FormGroup>
          </div>
          <div className="col">
            <FormGroup>
              <Button id='login' type="submit">
                Add
              </Button>
            </FormGroup>
          </div>
          <div className="col" />
        </div>
      </Form>
    </div>
  );
}
