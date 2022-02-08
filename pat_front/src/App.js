import 'react-app-polyfill/stable';
import React, {useState, useContext} from 'react';
import {
  HashRouter as Router,
  Switch,
  Route,
  Redirect
} from "react-router-dom";

import { makeStyles} from '@material-ui/core/styles';
import CssBaseline from '@material-ui/core/CssBaseline';
import { ThemeProvider  } from '@material-ui/core/styles';

import {lightTheme, darkTheme} from './app/theme';
import { UserContext } from "./app/user-context";
import WbNavbar from "./app/WbAppBar";
import WbDrawer from './app/WbDrawer';
import HomePage from './features/home-page';
import JobPage from './features/job-page';
import Login from './features/login-page';
import Settings from './features/setting-page';
import GuidePage from './features/guide-page';

const useStyles = makeStyles((theme) => ({
  root: {
    display: 'flex',
  },
  toolbar: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'flex-end',
    padding: theme.spacing(0, 1),
    minHeight: '64px',
  },
  content: {
    flexGrow: 1,
    padding: theme.spacing(3),
  },
}));

function App () {
  const classes = useStyles();
  const [user,] = useContext(UserContext);

  let prefTheme = localStorage.getItem('prefer_theme')
  if(!prefTheme)
  {
    prefTheme='light';
    if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
      prefTheme='dark';
    }
  }
  const [theme, setTheme] = useState(prefTheme) 
  
  const NotFound = ()=>{
    return <div className="float-left"><h2>404 Page Not Found!</h2></div>
  };

  const IsLogin = user.name.length> 0 && user.email.length > 0;
  return (
    <ThemeProvider  theme={theme === 'dark' ? darkTheme: lightTheme}>
      <Router>
        <div className={classes.root}>
          <CssBaseline />
          <WbNavbar theme={theme} />
          <WbDrawer />
          <main className={classes.content}>
            <div className={classes.toolbar} />
            <Switch>
                <Route exact path="/"
                  render={() => {return <Redirect to="/home" />}}/>
                <Route exact path="/home">
                  <HomePage />
                </Route>
                <Route exact path="/job/" >
                  {IsLogin?<JobPage theme={theme} />:<Login backto='/job'/>}
                </Route>
                <Route exact path="/guide">
                  <GuidePage theme={theme} />
                </Route>
                <Route exact path="/setting">
                  <Settings theme={theme} ChangeTheme={setTheme} />
                </Route>
                <Route exact path="/login">
                  <Login />
                </Route>
                <Route path="*">
                  <NotFound  />
                </Route>
              </Switch>
          </ main>
        </div>
      </Router>
    </ThemeProvider >
  );
}

export default App;
