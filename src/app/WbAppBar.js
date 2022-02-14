import React, {useContext} from 'react';
import {Link} from "react-router-dom";

import { makeStyles } from '@material-ui/core/styles';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Typography from '@material-ui/core/Typography';

import { UserContext } from "./user-context";

import {GcLogo} from '@gcui/react';
import logo from './gc-logo.png';


const useStyles = makeStyles((theme) => ({
  appBar: {
    zIndex: theme.zIndex.drawer + 1,
    transition: theme.transitions.create(['width', 'margin'], {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.leavingScreen,
    }),
  },
  toolbar: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'flex-end',
    padding: theme.spacing(0, 1),
    minHeight: '64px',
  },
  logoImage: {
    marginLeft: 10,
    marginRight: 50,
    height: 20,
  },
  title: {
    flexGrow: 1
  },
  menuLink: {
    color: 'inherit', 
    '&:hover':{
      color: theme.palette.text.secondary,
    },
    textDecoration: 'inherit',
    marginRight:'12px'
  },
}));


function WbNavbar() {
  const classes = useStyles();
  const [user,] = useContext(UserContext);

  const LoginLabel = ()=>{
    if(user.name.length > 0 )
      return "Welcome, "+ user.name + "!";
    else 
      return "Login";
  };

  return (
  <AppBar position="fixed" color='default' className={classes.appBar}>
    <Toolbar disableGutters >
      <img alt="GC Logo" src={logo} className={classes.logoImage}/>
      <Typography variant="h6" noWrap className={classes.title}>
          Premium Allocation Tool
      </Typography>
      <Link to="/login" className={classes.menuLink} >{LoginLabel()}</Link>
    </Toolbar>
  </AppBar>
  );
}

export default WbNavbar;
