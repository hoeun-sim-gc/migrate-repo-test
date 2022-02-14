import 'react-app-polyfill/stable';
import { createTheme, responsiveFontSizes } from '@material-ui/core/styles';
import React, {useState, useContext} from 'react';
import {
  HashRouter as Router,
    Route,
    Routes,
  Navigate
} from "react-router-dom";

import { makeStyles} from '@material-ui/core/styles';
import CssBaseline from '@material-ui/core/CssBaseline';
import { ThemeProvider  } from '@material-ui/core/styles';

import { UserContext } from "./app/user-context";
import WbNavbar from "./app/WbAppBar";
import WbDrawer2 from './app/WbDrawer2';
import HomePage from './features/home-page';
import JobPage from './features/job-page';
import Login from './features/login-page';
import Settings from './features/setting-page';
import GuidePage from './features/guide-page';
import {Options} from "./app/theme";

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
  
  console.log("USING THEME: " + theme);

  const NotFound = ()=>{
    return <div className="float-left"><h2>404 Page Not Found!</h2></div>
  };

  const IsLogin = user.name.length> 0 && user.email.length > 0;

  const gcTheme = createTheme(Options)

  const optionCopy = Object.assign({}, theme === "dark" ? {palette: {}} : Options);
  optionCopy["palette"]["type"] = theme
  //const muiTheme = theme === "dark" ? darkTheme : lightTheme;
  const muiTheme = createTheme(optionCopy);
  
  return (
    <ThemeProvider theme={muiTheme}>
      <Router>
        <div className={classes.root}>
          <CssBaseline />
          <WbNavbar />
          <WbDrawer2 />
          <main className={classes.content}>
            <div className={classes.toolbar} />
            <Routes>
                <Route exact path="/"
                  render={() => {return <Navigate to="/home" />}}/>
                <Route exact path="/home" element={<HomePage />}>
               
                </Route>
                <Route exact path="/job/" element ={IsLogin?<JobPage theme={theme} />:<Login backto='/job'/>}>
                </Route>
                <Route exesact path="/guide" element={ <GuidePage theme={theme} />}>
                 
                </Route>
                <Route exact path="/setting" element={  <Settings theme={theme} ChangeTheme={setTheme} />}>
                
                </Route>
                <Route exact path="/login" element={<Login/>}>
                </Route>
                <Route path="*" element={<NotFound />}>
                </Route>
              </Routes>
          </ main>
        </div>
      </Router>
    </ThemeProvider >
  );
}

export default App;
