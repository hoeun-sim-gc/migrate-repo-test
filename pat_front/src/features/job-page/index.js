import React, { useState, useContext, useRef } from 'react';
import { useParams, useHistory } from "react-router-dom";
import { makeStyles, useTheme } from '@material-ui/core/styles';
import {
  Grid, Card, CardContent,
  InputLabel, FormControl, Select,
  Typography, Button, TextField, Box,
  MenuList, MenuItem, Divider
} from '@material-ui/core';

import AdapterDateFns from '@mui/lab/AdapterDateFns';
import LocalizationProvider from '@mui/lab/LocalizationProvider';
import DatePicker from '@mui/lab/DatePicker';

import Tooltip from '@mui/material/Tooltip';

import Accordion from '@mui/material/Accordion';
import AccordionSummary from '@mui/material/AccordionSummary';
import AccordionDetails from '@mui/material/AccordionDetails';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import {
  Checkbox, RadioGroup, Radio,
  FormControlLabel,
  Dialog, DialogActions, DialogContent, DialogContentText, DialogTitle
} from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';

import ReactSpeedometer from "react-d3-speedometer"

import { PulseLoader } from "react-spinners";
import 'bootstrap/dist/css/bootstrap.min.css';
import 'react-bootstrap-table-next/dist/react-bootstrap-table2.min.css';

import { UserContext } from "../../app/user-context";

import "./index.css";
import ValidRules from "./valid-flag";

import { v4 as uuidv4 } from 'uuid';

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
    marginTop: '20px',
    marginBottom: '10px',
  },
  para: {
    color: theme.palette.text.primary,
    marginTop: 10,
    marginBottom: 10,
    overflow: "auto",
    padding: 5
  }
}));

export default function JobPage(props) {
  const { job_id } = useParams();
  const history = useHistory();
  const inputFile = useRef(null);

  const classes = useStyles();
  const theme = useTheme();
  const [user,] = useContext(UserContext);

  // current active job that showing the odometer; The parameter passes to the newjob 
  const [currentJob, setCurrentJob] = useState({ job_id: job_id ? job_id : 0, status: '', finished: 0 })
  const [loadingCurrentJob, setLoadingCurrentJob] = useState(false);
  const [refJob, setRefJob] = useState(null)

  const [newJob, setNewJob] = useState(
    {
      parameter: {
        job_name: "PAT_Test",
        job_guid: "",

        ref_analysis: 0,
        server: "",
        edm: "",
        rdm: "",
        portinfoid: 0,
        perilid: 0,
        analysisid: 0,
        
        data_correction: "",
        default_region: 0,
        valid_rules: 0,
        
        type_of_rating: "PSOLD",
        coverage: "Building + Contents + Time Element",
        peril_subline: "All Perils",
        loss_alae_ratio: 1,
        average_accident_date: "1/1/2022",
        trend_factor: 1.035,
        additional_coverage: 2.0,
        deductible_treatment: "Retains Limit",     

        user_name: user?.name,
        user_email: user?.email,
      },
      data_file: null,
      use_ref: false
    })
  const [paraString, setParaString] = useState('');
  const [jobList, setJobList] = useState([]);
  const [selectedNewJob, setSelectedNewJob] = useState('');
  

  const [inpExpanded, setInpExpanded] = useState(0x08);
  const [loadingServerList, setLoadingServerList] = useState(false);
  const [serverList, setServerList] = useState();

  const [loadingDbList, setLoadingDbList] = useState(false);
  const [dbList, setDbList] = useState({ rdm: [], edm: [] });

  const [loadingPortList, setLoadingPortList] = useState(false);
  const [portList, setPortList] = useState([]);

  const [loadingPerilList, setLoadingPerilList] = useState(false);
  const [perilList, setPerilList] = useState([]);

  const [loadingAnlsList, setLoadingAnlsList] = useState(false);
  const [anlsList, setAnlsList] = useState([]);

  const [uploadingJob, setUploadingJob] = useState(false);
  const [confirm, setConfirm] = React.useState(false);

  React.useEffect(() => {
    setLoadingServerList(true);

    setLoadingCurrentJob(true);
    const interval = setInterval(() => setLoadingCurrentJob(true), 30000);
    return () => {
      clearInterval(interval);
    };
  }, []);

  //server list , reference job
  React.useEffect(() => {
    if (!loadingServerList) return;

    //sever list
    let lst = [];
    let js = localStorage.getItem('Server_List')
    if (js) lst = JSON.parse(js);

    var svr = localStorage.getItem('currentServer');
    if (svr) {
      lst.push(svr)
      lst = [...new Set(lst)].sort();
      localStorage.setItem("Server_List", JSON.stringify(lst));
    }
    setServerList(lst);

    //reference 
    if (job_id && job_id > 0) {
      const request = '/api/job/' + job_id;
      fetch(request).then(response => {
        if (response.ok) {
          return response.json();
        }
        throw new TypeError("Oops, we haven't got data!");
      })
        .then(data => {
          var job = JSON.parse(data.parameters);
          if (data.data_extracted > 0){
            setRefJob({job_id: data.job_id, 
              data_flag: job.server+"|" + job.edm+"|" + job.rdm + "|" + job.portinfoid + "|" + job.perilid  + "|" + job.analysisid
            })
          };

          job['job_guid'] = '';
          job['user_name'] = user?.name;
          job['user_email'] = user?.email;
          job['data_correction'] = ''
          job['ref_analysis'] = 0;

          setNewJob({ parameter: {...newJob.parameter,...job}, data_file: null, use_ref:true });

          if ('server' in job) svr = job['server'];
          else svr = localStorage.getItem('currentServer');
          if (svr) {
            lst.push(svr)
            lst = [...new Set(lst)].sort();
            localStorage.setItem("Server_List", JSON.stringify(lst));

            setDbList([]);
            setLoadingDbList(true);
          }
        })
        .catch(error => {
          console.log(error);
        })
        .then(() => {
          setLoadingServerList(false);
        });
    }
    else {
      setNewJob({ ...newJob, parameter: { ...newJob.parameter, server: svr, edm: '', rdm: '', portinfoid: 0, perilid: 0, analysisid: 0 } });
      setDbList([]);
      setLoadingDbList(true);
    }
    setLoadingServerList(false);

    // eslint-disable-next-line
  }, [loadingServerList]);


  React.useEffect(() => {
    setParaString(JSON.stringify(newJob?.parameter, null, '  '))
  }, [newJob.parameter]);


  //get EDM/RDM list 
  React.useEffect(() => {
    if (!loadingDbList) return;
    if (!newJob.parameter.server) {
      setLoadingDbList(false);
      return;
    }

    const request = '/api/db_list/' + newJob.parameter.server;
    fetch(request).then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(data => {
        if (data && data.rdm) data.rdm = [''].concat(data.rdm);
        setDbList(data);
        if (!data.edm.includes(newJob.parameter.edm)) {
          setNewJob({ ...newJob, parameter: { ...newJob.parameter, edm: data.edm[0], portinfoid: 0, perilid: 0 } });
        }
        if (!data.rdm.includes(newJob.parameter.rdm)) {
          setNewJob({ ...newJob, parameter: { ...newJob.parameter, rdm: data.rdm[0], analysisid: 0 } });
        }
        setLoadingPortList(true);
        setLoadingAnlsList(true);
      })
      .catch(error => {
        console.log(error);
      })
      .then(() => {
        setLoadingDbList(false);
      });
    // eslint-disable-next-line
  }, [loadingDbList, newJob.parameter.server]);

  //get port list 
  React.useEffect(() => {
    if (!loadingPortList) return;
    if (!newJob.parameter.edm) {
      setLoadingPortList(false);
      return;
    }

    const request = '/api/port/' + newJob.parameter.server + '/' + newJob.parameter.edm;
    fetch(request).then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(data => {
        setPortList(data);
        if (data.length > 0) {
          if (data.filter(e => e.portinfoid === newJob.parameter.portinfoid).length <= 0) {
            setNewJob({ ...newJob, parameter: { ...newJob.parameter, portinfoid: data[0].portinfoid, perilid: 0 } })
          }
        }

        setLoadingPerilList(true);
      })
      .catch(error => {
        console.log(error);
      })
      .then(() => {
        setLoadingPortList(false);
      });
    // eslint-disable-next-line
  }, [loadingPortList, newJob.parameter.edm]);

  //get Peril list 
  React.useEffect(() => {
    if (!loadingPerilList) return;
    if (!newJob.parameter.edm || newJob.parameter.portinfoid <= 0) {
      setLoadingPerilList(false);
      return;
    }

    const request = '/api/peril/' + newJob.parameter.server + '/' + newJob.parameter.edm + '/' + newJob.parameter.portinfoid;
    fetch(request).then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(data => {
        setPerilList(data);
        if (data.length > 0) {
          if (!data.includes(newJob.parameter.perilid)) setNewJob({ ...newJob, parameter: { ...newJob.parameter, perilid: data[0] } })
        }

        setLoadingPerilList(true);
      })
      .catch(error => {
        console.log(error);
      })
      .then(() => {
        setLoadingPerilList(false);
      });
    // eslint-disable-next-line
  }, [loadingPerilList, newJob.parameter.portinfoid]);


  //get analyis list 
  React.useEffect(() => {
    if (!loadingAnlsList) return;
    if (!newJob.parameter.rdm) {
      setLoadingAnlsList(false);
      return;
    }

    const request = '/api/anls/' + newJob.parameter.server + '/' + newJob.parameter.rdm;
    fetch(request).then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(data => {
        if (data) data = [{ id: 0, name: '' }].concat(data);
        else data = [{ id: 0, name: '' }]

        if (data.filter(r => r.id === newJob.parameter.analysisid).length <= 0) {
          setNewJob({ ...newJob, parameter: { ...newJob.parameter, analysisid: 0 } })
        }

        setAnlsList(data);
      })
      .catch(error => {
        console.log(error);
      })
      .then(() => {
        setLoadingAnlsList(false);
      });
    // eslint-disable-next-line
  }, [loadingAnlsList, newJob.parameter.rdm]);

  const ValidateJob = (job) => {
    if (!job) return false;

    if (!job.server || !job.edm || job.portinfoid <= 0) {
      console.log("No input data!");
      return false;
    }

    if (job.perilid <= 0) {
      console.log("Need peril id!");
      return false;
    }

    if (job.loss_alae_ratio <= 0) {
      console.log("Loss ALAE error!");
      return false;
    }

    if (!job.job_name) {
      console.log("Need job name!");
      return false;
    }

    return true;
  };

  //submit job
  React.useEffect(() => {
    if (!uploadingJob) return;
    if (!jobList || jobList.length <= 0 ) {
      setUploadingJob(false);
      return;
    }

    jobList.forEach((job)=>{
      if(ValidateJob(job.parameter)) {
        
        let form_data = new FormData();
        let js = job.parameter;
        if(job.data_file) 
        {
          js['data_correction'] =job.data_file.name;
          form_data.append("data",job.data_file);
        }
  
        form_data.append('para', JSON.stringify(js));
        post_job(form_data);
      }
    });
    // eslint-disable-next-line
  }, [uploadingJob]);


  const post_job = (form_data) => {
    let request = '/api/job'
    fetch(request, {
      method: "POST",
      body: form_data
    }).then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new TypeError("Oops, Submit job failed!");
    })
      .then(data => {
        let res = data.match(/Analysis submitted: ([0-9]*)/gi);
        if (res && res.length > 0) {
          try {
            var aid = parseInt(res[0].substring(20, res[0].length))
            if (aid > 0) {
              setCurrentJob({ job_id: aid, status: '', finished: 0 });
              setLoadingCurrentJob(true);
              history.push('/job/' + aid)
            }
          }
          catch (err) { }
        }
      })
      .catch(error => {
        console.log(error);
      })
      .then(() => {
        setUploadingJob(false);
      });
  };

  //get current job status 
  let status_percent = {
    'received': 0,
    'error': 0,
    'cancelled': 0,
    'wait_to_start': 2,
    'started': 5,
    'extracting_data': 10,
    'checking_data': 30,
    'net_of_fac': 40,
    'allocating': 60,
    'upload_results': 90,
    'finished': 100,
    'error': 100
  }
  React.useEffect(() => {
    if (!loadingCurrentJob) return;
    if (!currentJob || currentJob.job_id <= 0) {
      setLoadingCurrentJob(false);
      return;
    }

    const request = '/api/job/' + currentJob.job_id;
    fetch(request).then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(data => {
        if (data.status) {
          var perc = data.status in status_percent ? status_percent[data.status] : 0;
          setCurrentJob({ ...currentJob, status: data.status, finished: perc })
        }
        else {
          setCurrentJob({ ...currentJob, status: '', finished: 0 })
        }

      })
      .catch(error => {
        console.log(error);
      })
      .then(() => {
        setLoadingCurrentJob(false);
      });
    // eslint-disable-next-line
  }, [loadingCurrentJob]);

  let peril_name = ["Earthquake", "Windstorm", "Severe Storm/Winterstorm", "Flood", "Wildfire", "Terrorism", "WorkersComp"]
  const similarity = (s1, s2) => {
    var longer = s1;
    var shorter = s2;
    if (s1.length < s2.length) {
      longer = s2;
      shorter = s1;
    }
    var longerLength = longer.length;
    if (longerLength === 0) {
      return 1.0;
    }
    return (longerLength - editDistance(longer, shorter)) / parseFloat(longerLength);
  };

  const editDistance = (s1, s2) => {
    s1 = s1.toLowerCase();
    s2 = s2.toLowerCase();

    var costs = [];
    for (var i = 0; i <= s1.length; i++) {
      var lastValue = i;
      for (var j = 0; j <= s2.length; j++) {
        if (i === 0)
          costs[j] = j;
        else {
          if (j > 0) {
            var newValue = costs[j - 1];
            if (s1.charAt(i - 1) !== s2.charAt(j - 1))
              newValue = Math.min(Math.min(newValue, lastValue),
                costs[j]) + 1;
            costs[j - 1] = lastValue;
            lastValue = newValue;
          }
        }
      }
      if (i > 0)
        costs[s2.length] = lastValue;
    }
    return costs[s2.length];
  };


  const handleConfirm = (isOK) => {
    var it = confirm
    setConfirm('');
    if (isOK) {
      if (it === "stop the current analysis") handleStopJob();
      else if (it === "start/rerun the current analysis") handleStartJob();
      else if (it === "submit jobs") setUploadingJob(true);
      else if (it === "delete the selected job") handleDelJob();
      else if (it === "update the selected job") handleUpdateJob();
    }
  };

  const handleUpdateJob = () => {
    var lst = jobList;
    if(lst){
      var sel= lst.find(j => j.parameter['job_guid'] === selectedNewJob);
      if(sel){
          sel.parameter = {...newJob.parameter, job_guid:selectedNewJob}
          sel.data_file = newJob.data_file;
          setJobList(lst);
      }
    }
  };

  const handleDelJob = () => {
    var lst = jobList;
    if(lst){
      var sel= lst.find(j => j.parameter['job_guid'] === selectedNewJob);
      if(sel){
          lst.pop(sel);
          setJobList(lst);
      }    
    }
  };

  const handleStopJob = () => {
    let request = '/api/stop/' + currentJob.job_id;
    fetch(request, { method: "POST" }).then(response => {
      if (response.ok) {
        setLoadingCurrentJob(true);
      }
    });
  };

  const handleStartJob = () => {
    let request = '/api/run/' + currentJob.job_id;
    fetch(request, { method: "POST" }).then(response => {
      if (response.ok) {
        setLoadingCurrentJob(true);
      }
    });
  };

  return (
    <div class="job_container">
      {(loadingServerList || loadingDbList || loadingPortList || loadingPerilList || loadingDbList || uploadingJob || loadingCurrentJob) &&
        <div className={classes.spinner}>
          <PulseLoader
            size={30}
            color={"#2BAD60"}
            loading={loadingServerList || loadingPortList || loadingPerilList || loadingDbList || loadingDbList || uploadingJob || loadingCurrentJob}
          />
        </div>
      }
      <div class="job_left_col job_container1">
        <div class="job_top_row">
          <Card className={classes.card} variant="outlined">
            <CardContent>
              <Grid container>
                <Grid item md={8}>
                  <Typography variant='h5' color="textSecondary" gutterBottom>
                    Current Analysys
                  </Typography>
                  <Typography variant='h6' color="textSecondary" gutterBottom>
                    {currentJob ? currentJob.job_id : ''}
                  </Typography>
                  <div className={classes.odometer} >
                    <ReactSpeedometer
                      width={250}
                      height={150}
                      marginTop={50}
                      marginBottom={50}
                      needleHeightRatio={0.7}
                      value={currentJob ? currentJob.finished : 0}
                      maxValue={100}
                      segments={4}
                      startColor="green"
                      endColor="blue"
                      needleColor="red"
                      needleTransitionDuration={2000}
                      needleTransition="easeElastic"
                      currentValueText={currentJob ? currentJob.status : 'A'}
                      currentValuePlaceholderStyle={'#{value}'}
                      textColor={theme.palette.text.primary}
                    />
                  </div>
                </Grid>
                <Grid md={4}>
                  <MenuList>
                    <MenuItem
                      onClick={() => { setLoadingCurrentJob(true); }}
                    >Refresh</MenuItem>
                    <Divider />
                    <MenuItem
                      disabled={!currentJob || currentJob.job_id <= 0 || currentJob.finished <= 0 || currentJob.finished >= 100}
                      onClick={() => {
                        setConfirm("stop the current analysis");
                      }}
                    >Stop Current</MenuItem>
                    <MenuItem
                      disabled={!currentJob || currentJob.job_id <= 0 || (currentJob.finished > 0 && currentJob.finished < 100)}
                      onClick={() => {
                        setConfirm("start/rerun the current analysis");
                      }}
                    >Start Current</MenuItem>
                  </MenuList>
                  <input type='file' id='file' ref={inputFile} style={{ display: 'none' }} onChange={(e) => {
                    if (e.target.files && e.target.files.length > 0) {
                      setUploadingJob(true);
                    }
                  }} />
                </Grid>
              </Grid>
            </CardContent>
          </Card>
          <Dialog
            open={confirm}
            onClose={() => { setConfirm(''); }}
            aria-labelledby="alert-dialog-title"
            aria-describedby="alert-dialog-description"
          >
            <DialogTitle id="alert-dialog-title">
              {"Warning"}
            </DialogTitle>
            <DialogContent>
              <DialogContentText id="alert-dialog-description">
                Do you really want to {confirm}?
              </DialogContentText>
            </DialogContent>
            <DialogActions>
              <Button style={{ color: 'black' }} onClick={() => { handleConfirm(true) }} autoFocus>Yes</Button>
              <Button style={{ color: 'black' }} onClick={() => { handleConfirm(false) }} > Cancel </Button>
            </DialogActions>
          </Dialog>
        </div>
        <div class='row' style={{marginLeft: '2px'}}>
          <Tooltip title="Add job to list">
            <Button style={{outline: 'none', height:'36px'}}
                onClick={(e) => {
                  var lst = jobList;
                  if (!lst) lst = []
                  var job = {
                    parameter: JSON.parse(JSON.stringify(newJob.parameter)),
                    data_file: newJob.data_file,
                    use_ref: newJob.use_ref
                  };
                  job.parameter['job_guid'] = uuidv4();
                  if (refJob && job.use_ref && 
                        job.parameter.server+"|" + job.parameter.edm+"|" + job.parameter.rdm +"|"+ job.parameter.portinfoid 
                        + "|" + job.parameter.perilid  + "|" + job.parameter.analysisid === refJob.data_flag) 
                    job.parameter.ref_analysis = parseInt(refJob.job_id);
                  else job.parameter.ref_analysis = 0;
                  lst.push(job);

                  setJobList(lst);
                  setSelectedNewJob(job.parameter.job_guid);
                }}
              >Add
            </Button>
          </Tooltip>
          <Tooltip title="Remove selected job from list">
            <Button style={{outline: 'none', height:'36px'}}
                disabled={!jobList || jobList.length <= 0 || !selectedNewJob || !jobList.find(j => j.parameter['job_guid'] === selectedNewJob) } 
                onClick={() => {
                  setConfirm("delete the selected job");
                }}
              >Remove
            </Button>
          </Tooltip>
          <Tooltip title="Update selected job with the new settings">
            <Button style={{outline: 'none', height:'36px'}}
                disabled={!jobList || jobList.length <= 0 || !selectedNewJob || !jobList.find(j => j.parameter['job_guid'] === selectedNewJob) } 
                onClick={() => {
                  setConfirm("update the selected job");
                }}
              >Update
            </Button>
          </Tooltip>
          <Divider orientation="vertical" flexItem />
          <Tooltip title="Submit jobs in the list to backend service">
            <Button style={{outline: 'none', height:'36px'}}
                disabled={!jobList || jobList.length <= 0}
                onClick={() => {
                  setConfirm("submit jobs");
                }}
              >Submit
            </Button>
          </Tooltip>
        </div>
        <div box class="job_bottom_row">
          <FormControl component="fieldset">
            <RadioGroup
              defaultValue=""
              value={selectedNewJob}
              name="radio-buttons-group"
              onChange={(event) => {
                var lst= jobList;
                if(lst){
                  var sel = lst.find(j => j.parameter['job_guid'] === event.target.value); 
                  if (sel){
                    var para= JSON.parse(JSON.stringify(sel.parameter));
                    para['job_guid'] = '';
                    setNewJob({parameter:para, use_ref:sel.use_ref, data_file:sel.data_file});
                    setSelectedNewJob(event.target.value);
                    console.log(event.target.value);
                  }
                }
              }}
            >
              {jobList?.map((j) => {
                return <FormControlLabel value={j.parameter['job_guid']} control={<Radio />} label={j.parameter['job_name']} />
              })}
            </RadioGroup>
          </FormControl>
        </div>
      </div>
      <div class="job_right_col">
        <div>
          <FormControl className={classes.formControl}>
                  <Box
                    component="form"
                    sx={{
                      '& > :not(style)': { m: 1, width: '62ch' }
                    }}
                    noValidate
                    autoComplete="off"
                  >
                    <TextField id="alae-basic" label="New Job" variant="standard"
                      value={newJob.parameter.job_name}
                      onChange={event => {
                        if (newJob.parameter.job_name !== event.target.value) {
                          setNewJob({ ...newJob, parameter: { ...newJob.parameter, job_name: event.target.value } });
                        }
                      }}
                    />
                  </Box>
                </FormControl>
                <div>
                <FormControl className={classes.formControl}>
                  <FormControlLabel control={
                    <Checkbox style={{ color: theme.palette.text.primary, background: theme.palette.background.default }}
                      disabled={!newJob || !newJob.parameter || !refJob ||   
                        newJob.parameter.server+"|" + newJob.parameter.edm+"|" + newJob.parameter.rdm +"|"+ newJob.parameter.portinfoid 
                          + "|" + newJob.parameter.perilid  + "|" + newJob.parameter.analysisid !== refJob.data_flag }
                      checked={newJob && newJob.use_ref && newJob.parameter && refJob &&   
                        newJob.parameter.server+"|" + newJob.parameter.edm+"|" + newJob.parameter.rdm +"|"+ newJob.parameter.portinfoid 
                          + "|" + newJob.parameter.perilid  + "|" + newJob.parameter.analysisid === refJob.data_flag }
                      onChange={event => {
                        setNewJob({ ...newJob, use_ref:event.target.checked });
                      }}
                    />} label={"Use raw data from the reference analysis " + refJob?.job_id}
                  />
                </FormControl>
                </div>
          <Accordion style={{ color: theme.palette.text.primary, background: theme.palette.background.default }}
            expanded={inpExpanded & 0x01}
            onChange={(event, isExpanded) => {
              if (isExpanded) setInpExpanded(inpExpanded | 0x01);
              else setInpExpanded(inpExpanded & ~0x01);
            }}>
            <AccordionSummary
              expandIcon={<ExpandMoreIcon />}
              aria-controls="other-content"
              id="other-header"
            >
              <Typography>Policy & Fac Input: </Typography>
            </AccordionSummary>
            <AccordionDetails>
              <div>
                <FormControl className={classes.formControl}>
                  <InputLabel shrink id="server-placeholder-label">
                    Server
                  </InputLabel>
                  <Select
                    labelId="server-placeholder-label"
                    id="server-placeholder"
                    value={newJob.parameter.server}
                    defaultValue={''}
                    onChange={event => {
                      if (newJob.parameter.server !== event.target.value) {
                        localStorage.setItem("currentServer", event.target.value);
                        setNewJob({ ...newJob, parameter: { ...newJob.parameter, server: event.target.value, rdm: '', edm: '', portinfoid: 0, analysisid: 0 } });
                        setLoadingDbList(true);
                      }
                    }}
                  >
                    {serverList?.map((n) => {
                      return <MenuItem value={n}>{ }{n}</MenuItem>
                    })}
                  </Select>
                </FormControl>
              </div>
              <div>
                <FormControl className={classes.formControl}>
                  <InputLabel shrink id="edm-placeholder-label">
                    <Button style={{ padding: 0 }}
                      onClick={() => {
                        if (newJob.parameter.rdm) {
                          let rdm = newJob.parameter.rdm.toLowerCase().replace('rdm', 'edm');
                          let s0 = 0;
                          let ee = ''
                          dbList.edm.forEach(edm => {
                            let s = similarity(rdm, edm.toLowerCase())
                            if (s > s0) {
                              s0 = s;
                              ee = edm
                            }
                          });

                          setNewJob({ ...newJob, parameter: { ...newJob.parameter, 'edm': ee } });
                          setLoadingPortList(true);
                        }
                      }}
                    >
                      EDM
                    </Button>
                  </InputLabel>
                  <Select
                    labelId="edm-placeholder-label"
                    id="edm-placeholder"
                    value={newJob.parameter.edm}
                    defaultValue={''}
                    onChange={event => {
                      if (newJob.parameter.edm !== event.target.value) {
                        setNewJob({ ...newJob, parameter: { ...newJob.parameter, edm: event.target.value, portinfoid: 0 } });
                        setLoadingPortList(true);
                      }
                    }}
                  >
                    {dbList.edm?.map((n) => {
                      return <MenuItem value={n}>{ }{n}</MenuItem>
                    })}
                  </Select>
                </FormControl>
                <FormControl className={classes.formControl}>
                  <InputLabel shrink id="port-placeholder-label">
                    Portfolio
                  </InputLabel>
                  <Select
                    labelId="port-placeholder-label"
                    id="port-placeholder"
                    value={newJob.parameter.portinfoid}
                    defaultValue={''}
                    onChange={event => {
                      if (newJob.parameter.portinfoid !== event.target.value) {
                        setNewJob({ ...newJob, parameter: { ...newJob.parameter, portinfoid: event.target.value } });
                        setLoadingPerilList(true);
                      }
                    }}
                  >
                    {portList?.map((p) => {
                      return <MenuItem value={p.portinfoid}>{ }{p.portinfoid > 0 ? '(' + p.portinfoid + ') ' + p.portname : ''}</MenuItem>
                    })}
                  </Select>
                </FormControl>
                <FormControl className={classes.formControl}>
                  <InputLabel shrink id="peril-placeholder-label">
                    Peril
                  </InputLabel>
                  <Select
                    labelId="peril-placeholder-label"
                    id="peril-placeholder"
                    value={newJob.parameter.perilid}
                    defaultValue={''}
                    onChange={event => {
                      if (newJob.parameter.perilid !== event.target.value) {
                        setNewJob({ ...newJob, parameter: { ...newJob.parameter, perilid: event.target.value } });
                      }
                    }}
                  >
                    {perilList?.map((p) => {
                      return <MenuItem value={p}>{ }{p > 0 && p <= peril_name.length ? '(' + p + ') ' + peril_name[p - 1] : p}</MenuItem>
                    })}
                  </Select>
                </FormControl>
              </div>
              <div>
                <FormControl className={classes.formControl}>
                  <InputLabel shrink id="rdm-placeholder-label">
                    <Button style={{ padding: 0 }}
                      onClick={() => {
                        let edm = newJob.parameter.edm.toLowerCase().replace('edm', 'rdm');
                        let s0 = 0;
                        let r = ''
                        dbList.rdm.forEach(rdm => {

                          let s = similarity(edm, rdm.toLowerCase())
                          if (s > s0) {
                            s0 = s;
                            r = rdm
                          }
                        });

                        setNewJob({ ...newJob, parameter: { ...newJob.parameter, 'rdm': r } });
                        setLoadingAnlsList(true);
                      }}

                    >
                      RDM
                    </Button>
                  </InputLabel>
                  <Select
                    labelId="rdm-placeholder-label"
                    id="rdm-placeholder"
                    value={newJob.parameter.rdm}
                    defaultValue={''}
                    onChange={event => {
                      if (newJob.parameter.rdm !== event.target.value) {
                        setNewJob({ ...newJob, parameter: { ...newJob.parameter, rdm: event.target.value, analysisid: 0 } });
                        setLoadingAnlsList(true);
                      }
                    }}
                  >
                    {dbList.rdm?.map((n) => {
                      return <MenuItem value={n}>{ }{n}</MenuItem>
                    })}
                  </Select>
                </FormControl>
                <FormControl className={classes.formControl}>
                  <InputLabel shrink id="anls-placeholder-label">
                    Spider Analysis
                  </InputLabel>
                  <Select
                    labelId="anls-placeholder-label"
                    id="anls-placeholder"
                    value={newJob.parameter.analysisid}
                    defaultValue={''}
                    onChange={event => {
                      if (newJob.parameter.analysisid !== event.target.value) {
                        setNewJob({ ...newJob, parameter: { ...newJob.parameter, analysisid: event.target.value } });
                      }
                    }}
                  >
                    {anlsList?.map((a) => {
                      return <MenuItem value={a.id}>{ }{a.id > 0 ? '(' + a.id + ') ' + a.name : ''}</MenuItem>
                    })}
                  </Select>
                </FormControl>
              </div>
            </AccordionDetails>
          </Accordion>

          <Accordion style={{ color: theme.palette.text.primary, background: theme.palette.background.default }}
            expanded={inpExpanded & 0x02}
            onChange={(event, isExpanded) => {
              if (isExpanded) setInpExpanded(inpExpanded | 0x02);
              else setInpExpanded(inpExpanded & ~0x02);
            }}>
            <AccordionSummary
              expandIcon={<ExpandMoreIcon />}
              aria-controls="other-content"
              id="other-header"
            >
              <Typography>Allocation Parameters: </Typography>
            </AccordionSummary>
            <AccordionDetails>
              <div>
                <FormControl className={classes.formControl}>
                  <InputLabel shrink id="rate-placeholder-label">
                    Type of Rating
                  </InputLabel>
                  <Select
                    labelId="rate-placeholder-label"
                    id="rate-placeholder"
                    value={newJob.parameter.type_of_rating}
                    defaultValue={'PSOLD'}
                    onChange={event => {
                      if (newJob.parameter.type_of_rating !== event.target.value) {
                        setNewJob({ ...newJob, parameter: { ...newJob.parameter, type_of_rating: event.target.value } });
                      }
                    }}
                  >
                    <MenuItem value='PSOLD'>PSOLD</MenuItem>
                  </Select>
                </FormControl>
                <FormControl className={classes.formControl}>
                <InputLabel shrink id="peril-placeholder-label">
                  Peril / Subline
                </InputLabel>
                <Select
                  labelId="peril-placeholder-label"
                  id="peril-placeholder"
                  value={newJob.parameter.peril_subline}
                  defaultValue={'All Perils'}
                  onChange={event => {
                    if (newJob.parameter.peril_subline !== event.target.value) {
                      setNewJob({ ...newJob, parameter: { ...newJob.parameter, peril_subline: event.target.value } });
                    }
                  }}
                >
                  {['Fire', 'Wind', 'Special Cause of Loss', 'All Perils']
                    .map((n) => {
                      return <MenuItem value={n}>{ }{n}</MenuItem>
                    })}
                </Select>
              </FormControl>
              <FormControl className={classes.formControl}>
                  <Box
                    component="form"
                    sx={{
                      '& > :not(style)': { m: 1, width: '18ch' },
                    }}
                    noValidate
                    autoComplete="off"
                  >
                    <TextField id="alae-basic" label="Loss & ALAE ratio" variant="standard" type='number'
                      value={newJob.parameter.loss_alae_ratio}
                      inputProps={{
                        maxLength: 13,
                        step: "0.1"
                      }}
                      onChange={event => {
                        if (newJob.parameter.loss_alae_ratio !== event.target.value) {
                          setNewJob({ ...newJob, parameter: { ...newJob.parameter, loss_alae_ratio: parseFloat(event.target.value) } });
                        }
                      }}
                    />
                  </Box>
                </FormControl>
              </div>
              <FormControl className={classes.formControl}>
                <InputLabel shrink id="coverage-placeholder-label">
                  Coverage
                </InputLabel>
                <Select
                  labelId="coverage-placeholder-label"
                  id="coverage-placeholder"
                  value={newJob.parameter.coverage}
                  defaultValue={'Building + Contents + Time Element'}
                  onChange={event => {
                    if (newJob.parameter.coverage !== event.target.value) {
                      setNewJob({ ...newJob, parameter: { ...newJob.parameter, coverage: event.target.value } });
                    }
                  }}
                >
                  {['Building + Contents + Time Element', 'Building + Contents']
                    .map((n) => {
                      return <MenuItem value={n}>{ }{n}</MenuItem>
                    })}
                </Select>
              </FormControl>
              <FormControl className={classes.formControl}>
                <Box
                  component="form"
                  sx={{
                    '& > :not(style)': { m: 1, width: '22ch' },
                  }}
                  noValidate
                  autoComplete="off"
                >
                  <TextField id="addl-basic" label="Additional Coverage" variant="standard" type='number'
                    value={newJob.parameter.additional_coverage}
                    onChange={event => {
                      if (newJob.parameter.additional_coverage !== event.target.value) {
                        setNewJob({ ...newJob, parameter: { ...newJob.parameter, additional_coverage: parseFloat(event.target.value) } });
                      }
                    }}
                  />
                </Box>
              </FormControl>
              <div>
                <FormControl className={classes.formControl}>
                  <LocalizationProvider dateAdapter={AdapterDateFns}>
                    <DatePicker
                      label="Average Accident Date"
                      value={newJob.parameter.average_accident_date}
                      format="MM/DD/YYYY"
                      onChange={(newValue) => {
                        setNewJob({ ...newJob, parameter: { ...newJob.parameter, average_accident_date: newValue.toLocaleDateString('en-US') } });
                      }}
                      renderInput={(params) => <TextField {...params} />}
                    />
                  </LocalizationProvider>
                </FormControl>
                <FormControl className={classes.formControl}>
                  <Box
                    component="form"
                    sx={{
                      '& > :not(style)': { m: 1, width: '18ch' },
                    }}
                    noValidate
                    autoComplete="off"
                  >
                    <TextField id="trend-basic" label="Trend Factor" variant="standard" type='number'
                      value={newJob.parameter.trend_factor}
                      inputProps={{
                        maxLength: 13,
                        step: "0.01"
                      }}
                      onChange={event => {
                        if (newJob.parameter.trend_factor !== event.target.value) {
                          setNewJob({ ...newJob, parameter: { ...newJob.parameter, trend_factor: parseFloat(event.target.value) } });
                        }
                      }}
                    />
                  </Box>
                </FormControl>
              </div>
              <div>
                <FormControl className={classes.formControl}>
                  <InputLabel shrink id="deductible-placeholder-label">
                    Deductible
                  </InputLabel>
                  <Select
                    labelId="deductible-placeholder-label"
                    id="deductible-placeholder"
                    value={newJob.parameter.deductible_treatment}
                    defaultValue={'Retains Limit'}
                    onChange={event => {
                      if (newJob.parameter.deductible_treatment !== event.target.value) {
                        setNewJob({ ...newJob, parameter: { ...newJob.parameter, deductible_treatment: event.target.value } });
                      }
                    }}
                  >
                    {['Retains Limit', 'Erodes Limit']
                      .map((n) => {
                        return <MenuItem value={n}>{ }{n}</MenuItem>
                      })}
                  </Select>
                </FormControl>
              </div>


            </AccordionDetails>
          </Accordion>
          <Accordion style={{ color: theme.palette.text.primary, background: theme.palette.background.default }}
            expanded={inpExpanded & 0x04}
            onChange={(event, isExpanded) => {
              if (isExpanded) setInpExpanded(inpExpanded | 0x04);
              else setInpExpanded(inpExpanded & ~0x04);
            }}>
            <AccordionSummary
              expandIcon={<ExpandMoreIcon />}
              aria-controls="corr-content"
              id="corr-header">
              <Typography>Data Validation</Typography>
            </AccordionSummary>
            <AccordionDetails>
              <FormControl className={classes.formControl}>
                <Box
                  component="form"
                  sx={{
                    '& > :not(style)': { m: 1, width: '40ch' }
                  }}
                  noValidate
                  autoComplete="off"
                >
                  <TextField id="alae-basic" label="Data Correction File" variant="standard" readOnly
                    value={(newJob.data_file? newJob.data_file.name : "No Correction")}
                  />
                </Box>
                <div class="row" style={{ alignItems: 'center' }} >
                  <div class="col-md-4 align-left vertical-align-top">
                    <Button variant="raised" component="label" className={classes.button}
                    > Add
                      <input hidden
                        className={classes.input}
                        style={{ display: 'none' }}
                        id="raised-button-file"
                        type="file"
                        onChange={(e) => {
                          if (e.target.files && e.target.files.length > 0) {
                            setNewJob({ ...newJob, data_file: e.target.files[0] })
                          }
                        }}
                      />
                    </Button>
                  </div>
                  <div class="col-md-4 align-left vertical-align-top">
                    <Button variant="raised" component="span" className={classes.button}
                      onClick={(e) => { setNewJob({ ...newJob, data_file:null }) }}>
                      Remove
                    </Button>
                  </div>
                </div>
              </FormControl>
              <div>
                <ul style={{ listStyleType: "none" }}>
                  <li>
                    <FormControl className={classes.formControl}>
                      <Box
                        component="form"
                        sx={{
                          '& > :not(style)': { m: 1, width: '24ch' }
                        }}
                        noValidate
                        autoComplete="off"
                      >
                        <TextField id="alae-basic" label="Default Rating Region" variant="standard"
                          value={newJob.parameter.default_region && newJob.parameter.default_region > 0 ? newJob.parameter.default_region : "None"}
                          onChange={event => {
                            if (newJob.parameter.default_region !== event.target.value) {
                              var reg = 0
                              try {
                                reg = parseInt(event.target.value)
                              }
                              catch {
                                reg = 0
                              }
                              setNewJob({ ...newJob, parameter: { ...newJob.parameter, default_region: reg } });
                            }
                          }}
                        />
                      </Box>
                    </FormControl>
                  </li>
                  {
                    Object.entries(ValidRules).map((n) => {
                      return <li><FormControlLabel control={
                        <Checkbox style={{ color: theme.palette.text.primary, background: theme.palette.background.default }}
                          checked={newJob.parameter.valid_rules & n[1].value}
                          onChange={event => {
                            var f = n[1].value;
                            if ((newJob.parameter.valid_rules & f !== 0) !== event.target.checked) {
                              setNewJob({
                                ...newJob, parameter: {
                                  ...newJob.parameter,
                                  valid_rules: event.target.checked ? newJob.parameter.valid_rules | f :
                                    newJob.parameter.valid_rules & (~f)
                                }
                              });
                            }
                          }}
                        />} label={n[1].descr}
                      /></li>
                    })}
                </ul>
              </div>
            </AccordionDetails>
          </Accordion>
          <Accordion style={{ color: theme.palette.text.primary, background: theme.palette.background.default }}
            expanded={inpExpanded & 0x08}
            onChange={(event, isExpanded) => {
              if (isExpanded) setInpExpanded(inpExpanded | 0x08);
              else setInpExpanded(inpExpanded & ~0x08);
            }}>
            <AccordionSummary
              expandIcon={<ExpandMoreIcon />}
              aria-controls="corr-content"
              id="corr-header">
              <Typography>Summary (Open each of the above tabs to edit)</Typography>
            </AccordionSummary>
            <AccordionDetails>
              <div>
                <textarea value={paraString}
                  readOnly={true}
                  style={{ width: '100%', height: '400px', color: theme.palette.text.primary, background: theme.palette.background.default }}>
                </textarea>
              </div>
            </AccordionDetails>
          </Accordion>
        </div>
      </div>
    </div>
  );
};