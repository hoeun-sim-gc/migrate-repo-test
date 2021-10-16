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
        var dt= new Date(Date.parse(utc)- new Date().getTimezoneOffset()* 60000*2); //fake time when convert to utc equals local
        return dt.toISOString().replace('T',' ').replace(/(\.[0-9]*Z)$/g, "");
    }
    catch (err){}
    return utc;
  };
