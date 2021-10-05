import React, { useState, useContext, useRef } from 'react';
import { makeStyles, useTheme } from '@material-ui/core/styles';
import {
  Grid, Card, CardContent,
  InputLabel, FormControl, FormControlLabel,
  Button, Select, Divider, Switch,
  Typography,
  MenuList, MenuItem
} from '@material-ui/core';
import ReactSpeedometer from "react-d3-speedometer"

import { PulseLoader } from "react-spinners";
import 'bootstrap/dist/css/bootstrap.min.css';
import 'react-bootstrap-table-next/dist/react-bootstrap-table2.min.css';

import { UserContext } from "../../app/user-context";
import columns from './header';
import WbMenu from '../../app/menu';

const useStyles = makeStyles((theme) => ({
  root: {
    flexGrow: 1,
  },
  toolbar: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'flex-end',
    padding: theme.spacing(0, 1),
    minHeight: '64px',
  },
  title: {
    flexGrow: 1,
    alignContent: 'center',
    textAlign: 'center',
    fontSize: 24,
  },
  menuLink: {
    color: 'inherit',
    textDecoration: 'inherit',
    marginRight: '12px'
  },
  paper: {
    height: 140,
    width: 100,
  },
  table: {
    color: theme.palette.text.primary,
    "&:hover tbody tr:hover td": {
      backgroundColor: theme.palette.action.selected,
      color: theme.palette.text.primary,
    }
  },
  formControl: {
    margin: theme.spacing(1),
    minWidth: 120,
  },
  spinner: {
    position: 'fixed',
    left: '50%',
    top: '50%',
    display: 'block',
    margin: '0 auto',
    zIndex: 9999,
  },
  card: {
    marginTop: 10,
    textAlign: 'left'
  },
  odometer: {
    marginTop: '30px',
    marginBottom: '30px',
  },
  para: {
    color: theme.palette.text.primary,
    marginTop: 10,
    marginBottom: 10,
    overflow: "auto",
    padding: 5
  },
}));

export default function JobPage(props) {
  const classes = useStyles();
  const theme = useTheme();

  const [user,] = useContext(UserContext);
  let savedServer = sessionStorage.getItem('currentServer')

  const [selectedServer, setSelectedServer] = useState(savedServer);
  
  const [loadingEdmList, setLoadingEdmList] = useState(false);
  const [selectedEDM, setSelectedEdm] = useState('');
  const [EdmList, setEdmList] = useState([]);

  const [loadingRdmList, setLoadingRdmList] = useState(false);
  const [selectedRDM, setSelectedRdm] = useState('');
  const [RdmList, setRdmList] = useState([]);

  const [loadingCurrentJob, setLoadingCurrentJob] = useState(false);
  const [currentJob, setCurrentJob] = useState(null);

  const [jobFiles, setJobFiles] = useState([])
  const [uploadingJob, setUploadingJob] = useState(false);

  const inputFile = useRef(null);
  const serverList = ['dfwcat-rms1sql1','dfwcat-rms4sql1'];

  React.useEffect(() => {
    setLoadingEdmList(true);

    // const interval = setInterval(() => setLoadingCurrentJob(true), 60000);
    // return () => {
    //   clearInterval(interval);
    // };
  }, []);

  //get EDM list 
  React.useEffect(() => {
    if (!loadingEdmList) return;
    if (selectedServer) {
      sessionStorage.setItem("currentServer", selectedServer);
    }

    // const request = '/api/WorkerList';
    // fetch(request).then(response => {
    //   if (response.ok) {
    //     return response.json();
    //   }
    //   throw new TypeError("Oops, we haven't got data!");
    // })
    //   .then(data => {
    //     var lst = data.map(x => { return x.Type }).filter((value, index, self) => {
    //       return self.indexOf(value) === index;
    //     });
    //     // setEdmList(data);
    //     // if (data.length > 0) {
    //     //   setServerList(lst);

    //     //   if (selectedServer) {
    //     //     let a = lst.find(u => u === selectedServer)
    //     //     setSelectedServer(a);
    //     //   }
    //     //   else setSelectedServer(lst[0]);
    //     // }
    //   })
    //   .catch(error => {
    //     console.log(error);
    //   })
    //   .then(() => {
        setLoadingEdmList(false);
    //   });
    // eslint-disable-next-line
  }, [loadingEdmList, selectedServer]);

  //   //get RDM list 
  //   React.useEffect(() => {
  //     if (!loadingEdmList) return;
  
  //     const request = '/api/WorkerList';
  //     fetch(request).then(response => {
  //       if (response.ok) {
  //         return response.json();
  //       }
  //       throw new TypeError("Oops, we haven't got data!");
  //     })
  //       .then(data => {
  //         var lst = data.map(x => { return x.Type }).filter((value, index, self) => {
  //           return self.indexOf(value) === index;
  //         });
  //         setRdmList(data);
  //         // if (data.length > 0) {
  //         //   setServerList(lst);
  
  //         //   if (selectedServer) {
  //         //     let a = lst.find(u => u === selectedServer)
  //         //     setSelectedServer(a);
  //         //   }
  //         //   else setSelectedServer(lst[0]);
  //         // }
  //       })
  //       .catch(error => {
  //         console.log(error);
  //       })
  //       .then(() => {
  //         setLoadingEdmList(false);
  //       });
  //     // eslint-disable-next-line
  //   }, [loadingRdmList, selectedServer]);

  // React.useEffect(() => {
  //   if (selectedEDM && EdmList.length > 0) {
  //     //trigger current job
  //     setLoadingCurrentJob(true);
  //   }
  // }, [selectedEDM, EdmList]);

  // //current job
  // React.useEffect(() => {
  //   if (!loadingCurrentJob) return;
  //   if (!selectedEDM) {
  //     setLoadingCurrentJob(false);
  //     return;
  //   }

  //   sessionStorage.setItem("currentWorker", selectedEDM)
  //   const request = '/api/Workers/' + selectedEDM + '/CurrentJob';
  //   fetch(request).then(response => {
  //     if (response.ok) {
  //       return response.json();
  //     }
  //     throw new TypeError("Oops, we haven't got data!");
  //   })
  //     .then(job => {
  //       ParseCurrentJob(job);
  //       setCurrentJob(job);
  //     })
  //     .catch(error => {
  //       console.log(error);
  //     })
  //     .then(() => {
  //       setLoadingCurrentJob(false);
  //     });
  //   // eslint-disable-next-line
  // }, [loadingCurrentJob, selectedEDM, EdmList]);

  // const ParseCurrentJob = (job) => {
  //   if (!job) return;
  //   job.finishPercent = 0;
  //   if (job.Info) {
  //     let res = job.Info.match(/Ranking progress: ([0-9]+.[0-9]*)%/gi);
  //     if (res.length > 0) {
  //       try {
  //         job.finishPercent = parseFloat(res[0].substring(18, res[0].length - 1))
  //       }
  //       catch (err) { }
  //     }
  //   }
  // };

  // //upload job
  // React.useEffect(() => {
  //   if (!uploadingJob) return;
  //   if (!jobFiles || jobFiles.length <= 0) {
  //     setUploadingJob(false);
  //     return;
  //   }

  //   let form_data = new FormData();
  //   for (var i = 0; i < jobFiles.length; i++) {
  //     form_data.append("file_" + i, jobFiles[i]);
  //   }

  //   let request = 'api/Jobs'
  //   fetch(request, {
  //     method: "POST",
  //     body: form_data
  //   }).then(response => {
  //     if (response.ok) {
  //       return response.json();
  //     }
  //     throw new TypeError("Oops, Submit job failed!");
  //   })
  //     .then(data => {
  //       var n = 0;
  //       for (var i = 0; i < data.length; i++) {
  //         if (data[i].AnalysisId > 0) n++;
  //       }
  //       if (n <= 0) {
  //         alert("Oops, Submit job failed! Please check the data file.");
  //       }
  //       else {
  //         if (jobFiles.length === 1) alert('New job has been submitted.');
  //         else alert('New job has been submitted (' + n + '/' + jobFiles.length + ' succeed).');

  //         setLoadingCurrentJob(true);
  //       }
  //     })
  //     .catch(error => {
  //       console.log(error);
  //     })
  //     .then(() => {
  //       setUploadingJob(false);
  //     });
  //   // eslint-disable-next-line
  // }, [uploadingJob, jobFiles]);

  return (
    <div>
      {(loadingEdmList || loadingRdmList || loadingCurrentJob || uploadingJob) &&
        <div className={classes.spinner}>
          <PulseLoader
            size={30}
            color={"#2BAD60"}
            loading={loadingEdmList || loadingRdmList || loadingCurrentJob || uploadingJob}
          />
        </div>
      }
      <Grid container className={classes.root} spacing={2}>
        <Grid item md={4} style={{ textAlign: 'left' }}>
          <Card className={classes.card} variant="outlined">
            <CardContent>
              <Grid container>
                <Grid item md={12}>
                  <Typography variant='h4' color="textSecondary" gutterBottom>
                    New Analysys
                  </Typography>
                  <div className={classes.odometer} >
                    <ReactSpeedometer
                      width={280}
                      height={180}
                      marginTop={50}
                      marginBottom={50}
                      needleHeightRatio={0.7}
                      value={currentJob?.finishPercent}
                      maxValue={100}
                      segments={4}
                      startColor="green"
                      endColor="blue"
                      needleColor="red"
                      needleTransitionDuration={2000}
                      needleTransition="easeElastic"
                      currentValueText="Not yet submitted"
                      currentValuePlaceholderStyle={'#{value}'}
                      textColor={theme.palette.text.primary}
                    />
                  </div>
                </Grid>
                <Grid md={4}>
                  <MenuList>
                    <MenuItem onClick={(e) => { setLoadingCurrentJob(true); }}>Submit Analysis</MenuItem>
                  </MenuList>
                </Grid>
              </Grid>
              <Divider />
              <div>
                <pre className={classes.para} style={{ Height: '100px', maxWidth: '1200px' }} >{currentJob?.job_id}</pre>
              </div>
            </CardContent>
          </Card>
          <input type='file' id='file' multiple ref={inputFile} style={{ display: 'none' }} onChange={(e) => {
            if (e.target.files && e.target.files.length > 0) {
              setJobFiles(e.target.files);
              setUploadingJob(true);
            }
          }} />
        </Grid>
        <Grid item md={8} style={{ textAlign: 'left' }}>
          <Grid container style={{ textAlign: 'left' }}>
            <Grid item md={4}>
              <FormControl className={classes.formControl}>
                <InputLabel shrink id="app-placeholder-label">
                  Server
                </InputLabel>
                <Select
                  labelId="server-placeholder-label"
                  id="server-placeholder"
                  value={selectedServer}
                  onChange={event => {
                    if (selectedServer !== event.target.value) {
                      setSelectedServer(event.target.value);
                    }
                  }}
                >
                  {serverList.map((n) => {
                    return <MenuItem value={n}>{}{n}</MenuItem>
                  })}
                </Select>
              </FormControl>
            </Grid>
            <Grid item md={4}>
              <FormControl className={classes.formControl}>
                <InputLabel shrink id="edm-placeholder-label">
                  EDM
                </InputLabel>
                <Select
                  labelId="edm-placeholder-label"
                  id="edm-placeholder"
                  value={selectedEDM}
                  onChange={event => {
                    if (selectedEDM !== event.target.value) {
                      setSelectedEdm(event.target.value);
                    }
                  }}
                >
                  {EdmList.filter((w) => w.Type === selectedServer).map((n) => {
                    return <MenuItem value={n}>{}{n}</MenuItem>
                  })}
                </Select>
              </FormControl>
            </Grid>
            <Grid item md={4}>
              <FormControl className={classes.formControl}>
                <InputLabel shrink id="rdm-placeholder-label">
                  RDM
                </InputLabel>
                <Select
                  labelId="rdm-placeholder-label"
                  id="rdm-placeholder"
                  value={selectedRDM}
                  onChange={event => {
                    if (selectedRDM !== event.target.value) {
                      setSelectedRdm(event.target.value);
                    }
                  }}
                >
                  {RdmList.filter((w) => w.Type === selectedServer).map((n) => {
                    return <MenuItem value={n.Name}>{}{n}</MenuItem>
                  })}
                </Select>
              </FormControl>
            </Grid>
          </Grid>
        </Grid>     
      </Grid>
    </div>
  );
};
