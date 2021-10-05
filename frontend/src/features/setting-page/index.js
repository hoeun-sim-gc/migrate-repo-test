import React from "react";
import Switch from '@material-ui/core/Switch';
import FormGroup from '@material-ui/core/FormGroup';
import FormControlLabel from '@material-ui/core/FormControlLabel';

export default function Settings(props) {
  const [checked, setChecked] = React.useState(props.theme==='dark');
  
  React.useEffect(()=>{
    const mode= checked?'dark':'light'
    props.ChangeTheme(mode);
    localStorage.setItem("prefer_theme",mode);
  },[checked, props]);

  const toggleChecked = () => {
    setChecked(!checked);
  };

  return (
    <FormGroup>
      <FormControlLabel
        control={<Switch checked={checked} onChange={toggleChecked} />}
        label="Browse in dark mode"
      />
    </FormGroup>
  );
}
