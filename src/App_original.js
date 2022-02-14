import logo from './logo.svg';
import './App.css';
import { theme } from "@gcui/react";
import { ThemeProvider } from '@material-ui/core';
import { makeStyles } from '@material-ui/core/styles';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Typography from '@material-ui/core/Typography';
import Button from '@material-ui/core/Button';
import IconButton from '@material-ui/core/IconButton';
import MenuIcon from '@material-ui/icons/Menu';

const useStyles = makeStyles((theme) => ({
  root: {
    flexGrow: 1,
  },
  menuButton: {
    marginRight: theme.spacing(2),
  },
  title: {
    flexGrow: 1,
  },
}));

function App_original() {
  const classes = useStyles();

  return (
    <ThemeProvider theme = { theme }>
    <div className = { classes.root }>
      <AppBar position = "static">
      <Toolbar>
      <IconButton edge = "start" className = { classes.menuButton } color = "inherit">
      <MenuIcon />
      </IconButton>
      <Typography variant = "h6"
        className = { classes.title }
        color = "inherit" >
        gcui-poc
        </Typography>
        <Button color = "inherit" > Login < /Button>
      </Toolbar>
      </AppBar>
      </div>
    <div className="App">
        <img src={logo} className="App-logo" alt="logo" />
        <p>
          Edit <code>src/App.js</code> and save to reload.
        </p>
        <p><Button variant = "contained" color = "secondary" >gcui-poc</Button></p>
        <a
          className="App-link"
          href="https://reactjs.org"
          target="_blank"
          rel="noopener noreferrer"
        >
          Learn React
        </a>
    </div>
    </ThemeProvider>
  );
}

export default App_original;