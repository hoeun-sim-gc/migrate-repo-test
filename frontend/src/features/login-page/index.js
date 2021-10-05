import React, { useState, useContext } from "react";
import { useHistory } from "react-router-dom";
import { Form, Button, FormGroup, FormControl } from "react-bootstrap";

import { UserContext } from "../../app/user-context";

import "./index.css";

export default function Login(props) {
    const [user, setUser] = useContext(UserContext);
    const history = useHistory()
    const [backto,] = useState(props.backto);
    
    const [newUser, setNewUser]=useState(
      {
        name: user.name,
        email: user.email
      });

    function handleSubmit(event) {
      event.preventDefault();

      if(newUser.name.length > 0 && newUser.email.length > 0) {
        setUser(newUser);
        localStorage.setItem('pat_user', JSON.stringify(newUser));
      }
      else {
        alert("Need to login!");
        return;
      }
      
      if(backto) history.push(backto);
      else history.goBack();//.push("/home");
    }
  
    return (
        <div className="Login">
          <form onSubmit={handleSubmit}>
            <FormGroup controlId="user name" bsSize="large">
              <label className="float-left">User:</label>
              <FormControl
                  value={newUser.name}
                  placeholder="Enter user name"
                  onChange={e => setNewUser({name:e.target.value, email:newUser.email})} />
            </FormGroup>
            <FormGroup controlId="user email" bsSize="large">
              <label className="float-left">Email:</label>
              <FormControl
                  value={newUser.email}
                  type="email" placeholder="Enter email"
                  onChange={e => setNewUser({name:newUser.name, email:e.target.value})} />
            </FormGroup>
            <Button id='login' block type="submit">
              Login
            </Button>
          </form>
        </div>
    );
}