import React, {useState} from 'react';
import {
  Link,
} from "react-router-dom";

import clsx from 'clsx';
import { makeStyles } from '@material-ui/core/styles';
import Drawer from '@material-ui/core/Drawer';
import List from '@material-ui/core/List';
import Divider from '@material-ui/core/Divider';
import IconButton from '@material-ui/core/IconButton';
import MenuIcon from '@material-ui/icons/Menu';
import ChevronLeftIcon from '@material-ui/icons/ChevronLeft';
import ListItem from '@material-ui/core/ListItem';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import ListItemText from '@material-ui/core/ListItemText';

import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faHome, faQuestionCircle,faCog,faPlus } from '@fortawesome/free-solid-svg-icons';

const drawerWidth = 200;
const useStyles = makeStyles((theme) => ({
  drawer: {
    width: drawerWidth,
    flexShrink: 0,
    whiteSpace: 'nowrap',
  },
  drawerOpen: {
    width: drawerWidth,
    transition: theme.transitions.create('width', {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.enteringScreen,
    }),
  },
  drawerClose: {
    transition: theme.transitions.create('width', {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.leavingScreen,
    }),
    overflowX: 'hidden',
    width: theme.spacing(7) + 1,
    [theme.breakpoints.up('sm')]: {
      width: theme.spacing(9) + 1,
    },
  },
  toolbar: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'flex-end',
    padding: theme.spacing(0, 1),
    minHeight: '64px',
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

function WbDrawer() {
  const classes = useStyles();
  const [expanded, setExpanded] = useState(false);
  
  const toggleExpanded = ()=> {
    setExpanded(!expanded);
  }

  return (
    <Drawer variant="permanent" className={clsx(classes.drawer, {
        [classes.drawerOpen]: expanded,
        [classes.drawerClose]: !expanded,
      })}
      classes={{
        paper: clsx({
          [classes.drawerOpen]: expanded,
          [classes.drawerClose]: !expanded,
        }),
      }}>
      <div className={classes.toolbar} />
      <IconButton onClick={toggleExpanded}>
        {expanded?<ChevronLeftIcon />:<MenuIcon />}
      </IconButton>
      <Divider />
      <List>
        <Link to='/home' className={classes.menuLink}>
          <ListItem button>
            <ListItemIcon><FontAwesomeIcon icon={faHome} className='fa-lg' /></ListItemIcon>
            <ListItemText primary='Home/Analyses' />
          </ListItem>
        </Link>
      </List>
      <Divider />
      <List>
        <Link to='/job' className={classes.menuLink}>
          <ListItem button>
            <ListItemIcon><FontAwesomeIcon icon={faPlus} className='fa-lg' /></ListItemIcon>
            <ListItemText primary='New Analysis' />
          </ListItem>
        </Link>
      </List>
      <Divider />
      <List>
        <Link to='/guide' className={classes.menuLink}>
          <ListItem button>
            <ListItemIcon><FontAwesomeIcon icon={faQuestionCircle} className='fa-lg' /></ListItemIcon>
            <ListItemText primary="Analysis Guide" />
          </ListItem>
        </Link>
      </List>
      <Divider />
      <List>
        <Link to='/setting' className={classes.menuLink}>
          <ListItem button>
            <ListItemIcon><FontAwesomeIcon icon={faCog} className='fa-lg' /></ListItemIcon>
            <ListItemText primary='Settings' />
          </ListItem>
        </Link>
      </List>
    </Drawer>
  );
}

export default WbDrawer;

