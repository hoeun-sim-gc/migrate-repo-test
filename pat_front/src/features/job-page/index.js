import React, { useState, useContext, useRef } from 'react';
import { useParams, useHistory } from "react-router-dom";
import { makeStyles, useTheme } from '@material-ui/core/styles';
import {
  Grid, Card, CardContent,
  InputLabel, FormControl, Select,
  Typography, Button, TextField, Box,
  MenuList, MenuItem
} from '@material-ui/core';

import AdapterDateFns from '@mui/lab/AdapterDateFns';
import LocalizationProvider from '@mui/lab/LocalizationProvider';
import DatePicker from '@mui/lab/DatePicker';

import Accordion from '@mui/material/Accordion';
import AccordionSummary from '@mui/material/AccordionSummary';
import AccordionDetails from '@mui/material/AccordionDetails';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import { Checkbox } from '@mui/material';
import FormControlLabel from '@mui/material/FormControlLabel';

import Dialog from '@mui/material/Dialog';
import DialogActions from '@mui/material/DialogActions';
import DialogContent from '@mui/material/DialogContent';
import DialogContentText from '@mui/material/DialogContentText';
import DialogTitle from '@mui/material/DialogTitle';

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
    marginTop: '30px',
    marginBottom: '30px',
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

  const [paraString, setParaString] = useState('');
  const [jobParameter, setJobParameter] = useState({
    server: '',
    edm: '',
    rdm: '',
    portinfoid: 0,
    perilid: 0,
    analysisid: 0,

    type_of_rating: 'PSOLD',
    peril_subline: 'All Perils',
    subject_premium: 1e8,
    coverage: 'Building + Contents + Time Element',
    loss_alae_ratio: 1.0,
    average_accident_date: '1/1/2022',
    trend_factor: 1.035,
    additional_coverage: 2.0,
    deductible_treatment: 'Retains Limit',
    data_correction: '',

    valid_rules: 0,
    default_region: 0,
    ref_analysis: 0,

    job_name: 'Test_PAT',
    user_name: user?.name,
    user_email: user?.email
  });

  const [inpExpanded, setInpExpanded] = useState(true);
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

  const [jobFile, setJobFile] = useState('')
  const [batchFile, setBatchFile] = useState('')
  const [uploadingJob, setUploadingJob] = useState(false);
  const [confirm, setConfirm] = React.useState(false);

  const [currentJob, setCurrentJob] = useState({ job_id: job_id, status: '', finished: 0 })
  const [loadingCurrentJob, setLoadingCurrentJob] = useState(false); 
  const [refJob, setRefJob] = useState(0)
  

  React.useEffect(() => {
    setLoadingServerList(true);

    setLoadingCurrentJob(true);
    const interval = setInterval(() => setLoadingCurrentJob(true), 10000);
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
      const request = '/api/para/' + job_id;
      fetch(request).then(response => {
        if (response.ok) {
          return response.json();
        }
        throw new TypeError("Oops, we haven't got data!");
      })
        .then(data => {
          delete data.job_guid;
          delete data.data_correction;
          data['ref_analysis'] = parseInt(job_id);
          data['user_name'] = user?.name;
          data['user_email'] = user?.email;

          setRefJob(parseInt(job_id));          
          setJobParameter(data);

          if ('server' in data) svr = data['server'];
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
      setJobParameter({ ...jobParameter, server: svr, edm: '', rdm: '', portinfoid: 0, perilid: 0, analysisid: 0 });
      setDbList([]);
      setLoadingDbList(true);
    }
    setLoadingServerList(false);

    // eslint-disable-next-line
  }, [loadingServerList]);


  React.useEffect(() => {
    setParaString(JSON.stringify(jobParameter, null, '  '))
  }, [jobParameter]);


  //get EDM/RDM list 
  React.useEffect(() => {
    if (!loadingDbList) return;
    if (!jobParameter.server) {
      setLoadingDbList(false);
      return;
    }

    const request = '/api/db_list/' + jobParameter.server;
    fetch(request).then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(data => {
        setDbList(data);
        if (!data.edm.includes(jobParameter.edm)) {
          setJobParameter({ ...jobParameter, edm: data.edm[0], portinfoid: 0, perilid: 0 });
        }
        if (!data.rdm.includes(jobParameter.rdm)) {
          setJobParameter({ ...jobParameter, rdm: data.rdm[0], analysisid: 0 });
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
  }, [loadingDbList, jobParameter.server]);

  //get port list 
  React.useEffect(() => {
    if (!loadingPortList) return;
    if (!jobParameter.edm) {
      setLoadingPortList(false);
      return;
    }

    const request = '/api/port/' + jobParameter.server + '/' + jobParameter.edm;
    fetch(request).then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(data => {
        setPortList(data);
        if (data.length > 0) {
          if (data.filter(e => e.portinfoid === jobParameter.portinfoid).length <= 0) {
            setJobParameter({ ...jobParameter, portinfoid: data[0].portinfoid, perilid: 0 })
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
  }, [loadingPortList, jobParameter.edm]);

  //get Peril list 
  React.useEffect(() => {
    if (!loadingPerilList) return;
    if (!jobParameter.edm || jobParameter.portinfoid <= 0) {
      setLoadingPerilList(false);
      return;
    }

    const request = '/api/peril/' + jobParameter.server + '/' + jobParameter.edm + '/' + jobParameter.portinfoid;
    fetch(request).then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(data => {
        setPerilList(data);
        if (data.length > 0) {
          if (!data.includes(jobParameter.perilid)) setJobParameter({ ...jobParameter, perilid: data[0] })
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
  }, [loadingPerilList, jobParameter.portinfoid]);


  //get analyis list 
  React.useEffect(() => {
    if (!loadingAnlsList) return;
    if (!jobParameter.rdm) {
      setLoadingAnlsList(false);
      return;
    }

    const request = '/api/anls/' + jobParameter.server + '/' + jobParameter.rdm;
    fetch(request).then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(data => {
        if (data) data = [{ id: 0, name: '' }].concat(data);
        else data = [{ id: 0, name: '' }]

        if (data.filter(r => r.id === jobParameter.analysisid).length <= 0) {
          setJobParameter({ ...jobParameter, analysisid: 0 })
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
  }, [loadingAnlsList, jobParameter.rdm]);

  const ValidateJob = () => {
    if (!jobParameter) return false;

    if (!jobParameter.server || !jobParameter.edm || jobParameter.portinfoid <= 0) {
      console.log("No input data!");
      return false;
    }

    if (jobParameter.perilid <= 0) {
      console.log("Need peril id!");
      return false;
    }

    if (jobParameter.subject_premium <= 0) {
      console.log("Need subject premium!");
      return false;
    }

    if (jobParameter.loss_alae_ratio <= 0) {
      console.log("Loss ALAE error!");
      return false;
    }

    if (!jobParameter.job_name) {
      console.log("Need job name!");
      return false;
    }

    return true;
  };

  //submit job
  React.useEffect(() => {
    if (!uploadingJob) return;
    if (!jobParameter) {
      setUploadingJob(false);
      return;
    }

    //if batch submit, ignore the data correction file and reference analysis   
    if (batchFile) {
      var fr = new FileReader();
      fr.onload = function () {
        var lns = fr.result.split(/\r?\n/g);
        var head = lns[0].split(',');
        var n = head.length;
        var js0 = jobParameter;
        js0['data_correction'] =''
        js0.ref_analysis = 0
        lns.slice(1).forEach(ln => {
          var col = ln.split(',');
          if (col.length === n) {
            var js = JSON.parse(JSON.stringify(js0));
            for (var i = 0; i < n; i++) {
              if (typeof (js[head[i]]) === 'number') js[head[i]] = Number(col[i]);
              else js[head[i]] = col[i];
            }

            let form_data = new FormData();
            js['job_guid'] = uuidv4();
            form_data.append('para', JSON.stringify(js));

            post_job(form_data)
          }
        });
      }
      fr.readAsText(batchFile);

    }
    else {
      let form_data = new FormData();
      let js = jobParameter;
      js['job_guid'] = uuidv4();
      delete js.portfolioid

      form_data.append('para', JSON.stringify(js));
      if (jobFile) form_data.append("data", jobFile);
      post_job(form_data)
    }

    // eslint-disable-next-line
  }, [uploadingJob]);


  const post_job = (form_data) => {
    let request = 'api/job'
    fetch(request, {
      method: "POST",
      body: form_data
    }).then(response => {
      if (response.ok) {
        return response.text();
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
    'stoppped': 0,
    'wait_to_start': 0,
    'started': 5,
    'extracting_data': 10,
    'checking_data': 30,
    'net_of_fac': 40,
    'allocating': 60,
    'upload_results': 90,
    'finished': 100
  }
  React.useEffect(() => {
    if (!loadingCurrentJob) return;
    if (!currentJob || currentJob.job_id <= 0) {
      setLoadingCurrentJob(false);
      return;
    }

    const request = '/api/status/' + currentJob.job_id;
    fetch(request).then(response => {
      if (response.ok) {
        return response.text();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(data => {
        if (data) {
          var perc = data in status_percent ? status_percent[data] : 0;
          setCurrentJob({ ...currentJob, status: data, finished: perc })
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

  const handleSubmit = (isOK) => {
    setConfirm(false);
    if (isOK && ValidateJob()) {
      setBatchFile('');
      setUploadingJob(true);
    }
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
                  <Typography variant='h4' color="textSecondary" gutterBottom>
                    Current Analysys
                  </Typography>
                  <Typography variant='h6' color="textSecondary" gutterBottom>
                    {currentJob && currentJob.job_id > 0 ? currentJob.job_id : ''}
                  </Typography>
                  <div className={classes.odometer} >
                    <ReactSpeedometer
                      width={280}
                      height={180}
                      marginTop={50}
                      marginBottom={50}
                      needleHeightRatio={0.7}
                      value={currentJob && currentJob.job_id > 0 ? currentJob.finished : 0}
                      maxValue={100}
                      segments={4}
                      startColor="green"
                      endColor="blue"
                      needleColor="red"
                      needleTransitionDuration={2000}
                      needleTransition="easeElastic"
                      currentValueText={currentJob && currentJob.job_id > 0 ? currentJob.status : 'Not yet submitted'}
                      currentValuePlaceholderStyle={'#{value}'}
                      textColor={theme.palette.text.primary}
                    />
                  </div>
                </Grid>
                <Grid md={4}>
                  <MenuList>
                    <MenuItem onClick={() => {
                      setConfirm(true);
                    }}>Submit Analysis</MenuItem>
                    <MenuItem onClick={(e) => {
                      if (ValidateJob()) inputFile.current.click();
                    }}>Batch Submit</MenuItem>
                  </MenuList>
                  <input type='file' id='file' ref={inputFile} style={{ display: 'none' }} onChange={(e) => {
                    if (e.target.files && e.target.files.length > 0) {
                      setBatchFile(e.target.files[0]);
                      setUploadingJob(true);
                    }
                  }} />
                </Grid>
              </Grid>
            </CardContent>
          </Card>
          <Dialog
            open={confirm}
            onClose={() => { setConfirm(false); }}
            aria-labelledby="alert-dialog-title"
            aria-describedby="alert-dialog-description"
          >
            <DialogTitle id="alert-dialog-title">
              {"Warning"}
            </DialogTitle>
            <DialogContent>
              <DialogContentText id="alert-dialog-description">
                Do you really want to submit a new analysis?
              </DialogContentText>
            </DialogContent>
            <DialogActions>
              <Button style={{ color: 'black' }} onClick={() => { handleSubmit(true) }} autoFocus>Yes</Button>
              <Button style={{ color: 'black' }} onClick={() => { handleSubmit(false) }} > Cancel </Button>
            </DialogActions>
          </Dialog>
        </div>
        <textarea value={paraString}
          readOnly={true}
          class="job_bottom_row"
          style={{ color: theme.palette.text.primary, background: theme.palette.background.default }}>
        </textarea>
      </div>
      <div class="job_right_col">
        <div>
          <Accordion expanded={inpExpanded}
            onChange={(event, isExpanded) => { setInpExpanded(isExpanded); }}
            style={{ color: theme.palette.text.primary, background: theme.palette.background.default }}>
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
                    value={jobParameter.server}
                    defaultValue={''}
                    onChange={event => {
                      if (jobParameter.server !== event.target.value) {
                        localStorage.setItem("currentServer", event.target.value);
                        setJobParameter({ ...jobParameter, server: event.target.value, rdm: '', edm: '', portinfoid: 0, analysisid: 0 });
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
                        if (jobParameter.rdm) {
                          let rdm = jobParameter.rdm.toLowerCase().replace('rdm', 'edm');
                          let s0 = 0;
                          let ee = ''
                          dbList.edm.forEach(edm => {
                            let s = similarity(rdm, edm.toLowerCase())
                            if (s > s0) {
                              s0 = s;
                              ee = edm
                            }
                          });

                          setJobParameter({ ...jobParameter, 'edm': ee });
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
                    value={jobParameter.edm}
                    defaultValue={''}
                    onChange={event => {
                      if (jobParameter.edm !== event.target.value) {
                        setJobParameter({ ...jobParameter, edm: event.target.value, portinfoid: 0 });
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
                    Porttfolio
                  </InputLabel>
                  <Select
                    labelId="port-placeholder-label"
                    id="port-placeholder"
                    value={jobParameter.portinfoid}
                    defaultValue={''}
                    onChange={event => {
                      if (jobParameter.portinfoid !== event.target.value) {
                        setJobParameter({ ...jobParameter, portinfoid: event.target.value });
                        setLoadingPerilList(true);
                      }
                    }}
                  >
                    {portList.map((p) => {
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
                    value={jobParameter.perilid}
                    defaultValue={''}
                    onChange={event => {
                      if (jobParameter.perilid !== event.target.value) {
                        setJobParameter({ ...jobParameter, perilid: event.target.value });
                      }
                    }}
                  >
                    {perilList.map((p) => {
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
                        let edm = jobParameter.edm.toLowerCase().replace('edm', 'rdm');
                        let s0 = 0;
                        let r = ''
                        dbList.rdm.forEach(rdm => {

                          let s = similarity(edm, rdm.toLowerCase())
                          if (s > s0) {
                            s0 = s;
                            r = rdm
                          }
                        });

                        setJobParameter({ ...jobParameter, 'rdm': r });
                        setLoadingAnlsList(true);
                      }}

                    >
                      RDM
                    </Button>
                  </InputLabel>
                  <Select
                    labelId="rdm-placeholder-label"
                    id="rdm-placeholder"
                    value={jobParameter.rdm}
                    defaultValue={''}
                    onChange={event => {
                      if (jobParameter.rdm !== event.target.value) {
                        setJobParameter({ ...jobParameter, rdm: event.target.value, analysisid: 0 });
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
                    value={jobParameter.analysisid}
                    defaultValue={''}
                    onChange={event => {
                      if (jobParameter.analysisid !== event.target.value) {
                        setJobParameter({ ...jobParameter, analysisid: event.target.value });
                      }
                    }}
                  >
                    {anlsList.map((a) => {
                      return <MenuItem value={a.id}>{ }{a.id > 0 ? '(' + a.id + ') ' + a.name : ''}</MenuItem>
                    })}
                  </Select>
                </FormControl>
              </div>
              <div>
                <FormControl className={classes.formControl}>
                  <FormControlLabel control={
                    <Checkbox style={{ color: theme.palette.text.primary, background: theme.palette.background.default }}
                      disabled = { refJob <= 0}
                      checked={jobParameter.ref_analysis && jobParameter.ref_analysis > 0}
                      onChange={event => {
                        if(event.target.checked) setJobParameter({...jobParameter, ref_analysis: refJob});
                        else setJobParameter({...jobParameter, ref_analysis: 0});
                      }}
                    />} label={"Use raw data from the reference analysis " + refJob}
                  />
                </FormControl>
              </div>
            </AccordionDetails>
          </Accordion>

          <Accordion style={{ color: theme.palette.text.primary, background: theme.palette.background.default }}>
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
                    value={jobParameter.type_of_rating}
                    defaultValue={'PSOLD'}
                    onChange={event => {
                      if (jobParameter.type_of_rating !== event.target.value) {
                        setJobParameter({ ...jobParameter, type_of_rating: event.target.value });
                      }
                    }}
                  >
                    <MenuItem value='PSOLD'>PSOLD</MenuItem>
                  </Select>
                </FormControl>
              </div>
              <FormControl className={classes.formControl}>
                <InputLabel shrink id="coverage-placeholder-label">
                  Coverage
                </InputLabel>
                <Select
                  labelId="coverage-placeholder-label"
                  id="coverage-placeholder"
                  value={jobParameter.coverage}
                  defaultValue={'Building + Contents + Time Element'}
                  onChange={event => {
                    if (jobParameter.coverage !== event.target.value) {
                      setJobParameter({ ...jobParameter, coverage: event.target.value });
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
                <InputLabel shrink id="peril-placeholder-label">
                  Peril / Subline
                </InputLabel>
                <Select
                  labelId="peril-placeholder-label"
                  id="peril-placeholder"
                  value={jobParameter.peril_subline}
                  defaultValue={'All Perils'}
                  onChange={event => {
                    if (jobParameter.peril_subline !== event.target.value) {
                      setJobParameter({ ...jobParameter, peril_subline: event.target.value });
                    }
                  }}
                >
                  {['Fire', 'Wind', 'Special Cause of Loss', 'All Perils']
                    .map((n) => {
                      return <MenuItem value={n}>{ }{n}</MenuItem>
                    })}
                </Select>
              </FormControl>
              <div>
                <FormControl className={classes.formControl}>
                  <Box
                    component="form"
                    sx={{
                      '& > :not(style)': { m: 1, width: '22ch' },
                    }}
                    noValidate
                    autoComplete="off"
                  >
                    <TextField id="premium-basic" label="Subject Premium" variant="standard"
                      value={jobParameter.subject_premium?.toLocaleString(
                        undefined, // leave undefined to use the visitor's browser 
                        // locale or a string like 'en-US' to override it.
                        { minimumFractionDigits: 0 }
                      )}
                      onChange={event => {
                        if (jobParameter.subject_premium !== event.target.value) {
                          setJobParameter({ ...jobParameter, subject_premium: parseFloat(event.target.value) });
                        }
                      }}
                    />
                  </Box>
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
                    <TextField id="alae-basic" label="Loss & ALAE ratio" variant="standard"
                      value={jobParameter.loss_alae_ratio}
                      onChange={event => {
                        if (jobParameter.loss_alae_ratio !== event.target.value) {
                          setJobParameter({ ...jobParameter, loss_alae_ratio: event.target.value });
                        }
                      }}
                    />
                  </Box>
                </FormControl>
              </div>
              <div>
                <FormControl className={classes.formControl}>
                  <LocalizationProvider dateAdapter={AdapterDateFns}>
                    <DatePicker
                      label="Average Accident Date"
                      value={jobParameter.average_accident_date}
                      format="MM/DD/YYYY"
                      onChange={(newValue) => {
                        setJobParameter({ ...jobParameter, average_accident_date: newValue.toLocaleDateString('en-US') });
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
                    <TextField id="trend-basic" label="Trend Factor" variant="standard"
                      value={jobParameter.trend_factor}
                      onChange={event => {
                        if (jobParameter.trend_factor !== event.target.value) {
                          setJobParameter({ ...jobParameter, trend_factor: event.target.value });
                        }
                      }}
                    />
                  </Box>
                </FormControl>
              </div>
              <div>
                <FormControl className={classes.formControl}>
                  <Box
                    component="form"
                    sx={{
                      '& > :not(style)': { m: 1, width: '22ch' },
                    }}
                    noValidate
                    autoComplete="off"
                  >
                    <TextField id="addl-basic" label="Additional Coverage" variant="standard"
                      value={jobParameter.additional_coverage}
                      onChange={event => {
                        if (jobParameter.additional_coverage !== event.target.value) {
                          setJobParameter({ ...jobParameter, additional_coverage: event.target.value });
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
                    value={jobParameter.deductible_treatment}
                    defaultValue={'Retains Limit'}
                    onChange={event => {
                      if (jobParameter.deductible_treatment !== event.target.value) {
                        setJobParameter({ ...jobParameter, deductible_treatment: event.target.value });
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
          <Accordion style={{ color: theme.palette.text.primary, background: theme.palette.background.default }}>
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
                    value={jobFile ? jobFile.name : "No Correction"}
                  />
                </Box>
                <div class="row" style={{ alignItems: 'center' }} >
                  <div class="col-md-4 align-left vertical-align-top">
                    <Button variant="raised" component="label" className={classes.button}>
                      Add
                      <input hidden
                        className={classes.input}
                        style={{ display: 'none' }}
                        id="raised-button-file"
                        type="file"
                        onChange={(e) => {
                          if (e.target.files && e.target.files.length > 0) {
                            setJobFile(e.target.files[0]);
                            setJobParameter({ ...jobParameter, data_correction: e.target.files[0].name })
                          }
                        }}
                      />
                    </Button>
                  </div>
                  <div class="col-md-4 align-left vertical-align-top">
                    <Button variant="raised" component="span" className={classes.button}
                      onClick={(e) => { setJobFile(''); setJobParameter({ ...jobParameter, data_correction: '' }) }}>
                      Remove
                    </Button>
                  </div>
                </div>
              </FormControl>
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
                    value={jobParameter.default_region && jobParameter.default_region > 0 ? jobParameter.default_region : "None"}
                    onChange={event => {
                      if (jobParameter.default_region !== event.target.value) {
                        var reg = 0
                        try{
                          reg = parseInt(event.target.value)
                        }
                        catch{
                          reg = 0
                        }
                        setJobParameter({ ...jobParameter, default_region: reg });
                      }
                    }}
                  />
                </Box>
              </FormControl>
              <div>
                <ul style={{ listStyleType: "none" }}>
                  {
                    Object.entries(ValidRules).map((n) => {
                      return <li><FormControlLabel control={
                        <Checkbox style={{ color: theme.palette.text.primary, background: theme.palette.background.default }}
                          checked={jobParameter.valid_rules & n[1].value}
                          onChange={event => {
                            var f = n[1].value;
                            if ((jobParameter.valid_rules & f !== 0) !== event.target.checked) {
                              setJobParameter({
                                ...jobParameter, valid_rules:
                                  event.target.checked ? jobParameter.valid_rules | f :
                                    jobParameter.valid_rules & (~f)
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
          <Accordion style={{ color: theme.palette.text.primary, background: theme.palette.background.default }}>
            <AccordionSummary
              expandIcon={<ExpandMoreIcon />}
              aria-controls="corr-content"
              id="corr-header">
              <Typography>Job Submit</Typography>
            </AccordionSummary>
            <AccordionDetails>
              <FormControl className={classes.formControl}>
                <Box
                  component="form"
                  sx={{
                    '& > :not(style)': { m: 1, width: '58ch' }
                  }}
                  noValidate
                  autoComplete="off"
                >
                  <TextField id="alae-basic" label="Job Name" variant="standard"
                    value={jobParameter.job_name}
                    onChange={event => {
                      if (jobParameter.job_name !== event.target.value) {
                        setJobParameter({ ...jobParameter, job_name: event.target.value });
                      }
                    }}
                  />
                </Box>
              </FormControl>
            </AccordionDetails>
          </Accordion>
        </div>
      </div>
    </div>
  );
};