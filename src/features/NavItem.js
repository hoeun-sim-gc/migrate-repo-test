import React, {useState} from 'react';
import ListItem from '@material-ui/core/ListItem';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import ListItemText from '@material-ui/core/ListItemText';
import List from '@material-ui/core/List';
import {
    Link,
  } from "react-router-dom";
  import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';


export default function NavItem(props) {
  return (
    <List>
      <Link to={'/' + props.linkName} className={props.linkStyle}>
        <ListItem button>
          <ListItemIcon>
            <FontAwesomeIcon icon={props.icon} className="fa-lg" />
          </ListItemIcon>
          <ListItemText primary={props.linkText} />
        </ListItem>
      </Link>
    </List>
  );
}
