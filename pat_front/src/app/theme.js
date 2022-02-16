
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

export const Options = {
  palette: {
    primary: {
      main: '#002c77',
      light: '#0065AC',
      dark: '#001F52',
      contrastText: '#FFFFFF',
    },
    secondary: {
      main: '#009de0',
      light: '#3bb8f0',
      dark: '#0065AC',
      contrastText: '#FFFFFF',
    },
    error: {
      main: '#C53532',
      light: '#EF4E45',
      dark: '#9A1C1F',
      contrastText: '#FFFFFF',
    },
    warning: {
      main: '#FFBE00',
      light: '#FFD240',
      dark: '#C98600',
      contrastText: '#202020',
    },
    info: {
      main: '#8096B2',
      light: '#A2B7CD',
      dark: '#627798',
      contrastText: '#FFFFFF',
    },
    success: {
      main: '#14853D',
      light: '#00AC41',
      dark: '#275D38',
      contrastText: '#FFFFFF',
    },
    text: {
      primary: 'rgba(32, 32, 32, 0.87)',
      secondary: 'rgba(32, 32, 32, 0.54)',
      disabled: 'rgba(32, 32, 32, 0.38)',
    },
    action: {
      active: 'rgba(32, 32, 32, 0.54)',
      hover: 'rgba(32, 32, 32, 0.04)',
      selected: 'rgba(32, 32, 32, 0.08)',
      disabled: 'rgba(32, 32, 32, 0.26)',
      disabledBackground: 'rgba(32, 32, 32, 0.12)',
    },
    background: {
      default: 'rgba(240, 240, 240, 1)',
      paper: 'rgba(255, 255, 255, 1)',
    },
    divider: 'rgba(32, 32, 32, 0.12)',
    contrastThreshold: 3,
    tonalOffset: 0.2,
  },
  typography: {
    fontFamily: '"Arial", sans-serif',
    h1: {
      fontFamily: '"Arial", sans-serif',
      fontWeight: 'normal',
      fontSize: '96px',
      lineHeight: '128px',
      letterSpacing: '-1.5px',
      color: 'rgba(32, 32, 32, 0.87)',
    },
    h2: {
      fontFamily: '"Arial", sans-serif',
      fontWeight: 'normal',
      fontSize: '60px',
      lineHeight: '80px',
      letterSpacing: '0.5px',
      color: 'rgba(32, 32, 32, 0.87)',
    },
    h3: {
      fontFamily: '"Arial", sans-serif',
      fontWeight: 'normal',
      fontSize: '48px',
      lineHeight: '64px',
      letterSpacing: '0',
      color: 'rgba(32, 32, 32, 0.87)',
    },
    h4: {
      fontFamily: '"Arial", sans-serif',
      fontWeight: 'normal',
      fontSize: '34px',
      lineHeight: '48px',
      letterSpacing: '0.25px',
      color: 'rgba(32, 32, 32, 0.87)',
    },
    h5: {
      fontFamily: '"Arial", sans-serif',
      fontWeight: 'normal',
      fontSize: '24px',
      lineHeight: '32px',
      letterSpacing: '0',
      color: 'rgba(0, 44, 119, 1)',
    },
    h6: {
      fontFamily: '"Arial", sans-serif',
      fontWeight: 'bold',
      fontSize: '20px',
      lineHeight: '26px',
      letterSpacing: '0.15px',
      color: 'rgba(0, 157, 224, 1)',
    },
    subtitle1: {
      fontFamily: '"Arial", sans-serif',
      fontWeight: 'normal',
      fontSize: '16px',
      lineHeight: '28px',
      letterSpacing: '0.15px',
      color: 'rgba(0,157,224,1)',
    },
    subtitle2: {
      fontFamily: '"Arial", sans-serif',
      fontWeight: 'bold',
      fontSize: '14px',
      lineHeight: '24px',
      letterSpacing: '0.1px',
      color: 'rgba(32, 32, 32, 0.87)',
    },
    body1: {
      fontFamily: '"Arial", sans-serif',
      fontWeight: 'normal',
      fontSize: '16px',
      lineHeight: '24px',
      letterSpacing: '0',
      color: 'rgba(32, 32, 32, 0.87)',
    },
    body2: {
      fontFamily: '"Arial", sans-serif',
      fontWeight: 'normal',
      fontSize: '14px',
      lineHeight: '20px',
      letterSpacing: '0',
      color: 'rgba(32, 32, 32, 0.87)',
    },
    button: {
      fontFamily: '"Arial", sans-serif',
      fontWeight: 'bold',
      fontSize: '14px',
      letterSpacing: '1.25px',
      color: 'rgba(32, 32, 32, 0.87)',
    },
    caption: {
      fontFamily: '"Arial", sans-serif',
      fontWeight: 'normal',
      fontSize: '12px',
      lineHeight: '20px',
      letterSpacing: '0',
      color: 'rgba(32, 32, 32, 0.87)',
    },
    overline: {
      fontFamily: '"Arial", sans-serif',
      fontWeight: 'normal',
      fontSize: '11px',
      lineHeight: '20px',
      letterSpacing: '1.5px',
      color: 'rgba(32, 32, 32, 0.87)',
    },
  },
};

