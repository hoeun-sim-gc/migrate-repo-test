import { createTheme, responsiveFontSizes } from '@material-ui/core/styles';

export const lightTheme = responsiveFontSizes(createTheme({   
    palette: {
        type: 'light'
    } 
}));

export const darkTheme = responsiveFontSizes(createTheme({
    palette: {
        type: 'dark'
    } 
}));

export const convertTime = (utc) => {
    try {
        var dt= new Date(Date.parse(utc)- new Date().getTimezoneOffset()* 60000*2); //fake time when convert to utc equals local
        return dt.toISOString().replace('T',' ').replace(/(\.[0-9]*Z)$/g, "");
    }
    catch (err){}
    return utc;
  };

  export const calcDuration = (t1,t2) => {
    try {
        var diff = (Date.parse(t2)- Date.parse(t1));
        if (isNaN(diff)) {
            return null;
          }

        var hours = Math.floor(diff / (1000 * 60 * 60));
        diff -= hours * (1000 * 60 * 60);
        
        var mins = Math.floor(diff / (1000 * 60));
        diff -= mins * (1000 * 60);
        
        var seconds = diff * 1e-3

        return hours.toString().padStart(2, '0') + ":" + mins.toString().padStart(2, '0') + ":" + seconds.toFixed(0);
    }
    catch (err){}
    return null;
  };


  
