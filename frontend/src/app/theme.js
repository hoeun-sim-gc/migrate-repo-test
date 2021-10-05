import { createTheme } from '@material-ui/core/styles';

export const lightTheme = createTheme({   
    palette: {
        type: 'light'
    } 
});

export const darkTheme = createTheme({
    palette: {
        type: 'dark'
    } 
});

export const convertTime = (utc) => {
    try {
        //force to conver to local string
        var dt= new Date(Date.parse(utc)- new Date().getTimezoneOffset()* 60000); //fake time
        return dt.toISOString().replace('T',' ').replace(/(\.[0-9]*Z)$/g, "");
    }
    catch (err){}
    return utc;
  };
