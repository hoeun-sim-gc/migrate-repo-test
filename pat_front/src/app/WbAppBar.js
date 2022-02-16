import React, {useContext} from 'react';
import {Link} from "react-router-dom";

import { makeStyles } from '@material-ui/core/styles';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Typography from '@material-ui/core/Typography';

import { UserContext } from "./user-context";


const useStyles = makeStyles((theme) => ({
  appBar: {
    backgroundColor: 'rgba(240, 240, 240, 1)',
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
    flexGrow: 1,
    fontFamily: '"Arial", sans-serif',
    fontWeight: 'bold',
    fontSize: '20px',
    lineHeight: '26px',
    letterSpacing: '0.15px',
    color: '#002c77',

  },
  menuLink: {
    color: 'rgba(32, 32, 32, 0.87)', 
    '&:hover':{
      color: theme.palette.text.secondary,
    },
    textDecoration: 'inherit',
    marginRight:'12px',
    fontFamily: '"Arial", sans-serif',
    fontWeight: 'bold',
    fontSize: '14px',
    lineHeight: '24px',
    letterSpacing: '1.25px',
    
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
  let prefTheme = localStorage.getItem('prefer_theme')

  return (
      <AppBar style={{backgroundColor: prefTheme === 'dark'? '#A9A9A9':'rgba(240, 240, 240)'}} className={classes.appBar}>
        <Toolbar disableGutters >
          <img alt='GC Logo' src={require('./gc-logo-new.png')} className={classes.logoImage}/>
          <Typography noWrap className={classes.title}>
              Premium Allocation Tool
          </Typography>
          <Link to="/login" className={classes.menuLink} >{LoginLabel()}</Link>
        </Toolbar>
      </AppBar>
  );
}

export default WbNavbar;

