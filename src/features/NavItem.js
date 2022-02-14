import React, {useState} from 'react';
import ListItem from '@material-ui/core/ListItem';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import ListItemText from '@material-ui/core/ListItemText';
import List from '@material-ui/core/List';
import {
    Link,
  } from "react-router-dom";
  import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

import { ThemeProvider  } from '@material-ui/core/styles';

import {Options} from "../app/theme";


export default function NavItem(props) {
  let prefTheme = localStorage.getItem('prefer_theme')
  if(!prefTheme)
  {
    prefTheme='light';
    if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
      prefTheme='dark';
    }
  }
  const [theme, setTheme] = useState(prefTheme) 

  const optionCopy = Object.assign({}, theme === "dark" ? {palette: {}} : Options);
  optionCopy["palette"]["type"] = theme
  //const muiTheme = theme === "dark" ? darkTheme : lightTheme;
  // const muiTheme = createTheme(optionCopy);
  
  return (
    <ThemeProvider theme={theme}>
      <List>
        <Link to={'/' + props.linkName} className={props.linkStyle}>
          <ListItem button>
            <ListItemIcon>
              <FontAwesomeIcon icon={props.icon} className="fa-lg" />
            </ListItemIcon>
            <ListItemText primary={props.linkText}/>
          </ListItem>
        </Link>
      </List>
    </ThemeProvider>
  );
}
