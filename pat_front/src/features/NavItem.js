import React, {useState} from 'react';
import ListItem from '@material-ui/core/ListItem';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import ListItemText from '@material-ui/core/ListItemText';
import List from '@material-ui/core/List';
import {
    Link,
  } from "react-router-dom";
  import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { makeStyles } from '@material-ui/core/styles';



const useStyles = makeStyles((theme) => {
return ({
  sideNavTitle: theme.typography.subtitle1
})});


export default function NavItem(props) {
  const classes = useStyles();
  
  return (
      <List>
        <Link to={'/' + props.linkName} className={props.linkStyle}>
          <ListItem button>
            <ListItemIcon>
              <FontAwesomeIcon icon={props.icon} className="fa-lg" />
            </ListItemIcon>
            <ListItemText disableTypography primary={props.linkText} className={classes.sideNavTitle} /> 
          </ListItem>
        </Link>
      </List>
  );
}
