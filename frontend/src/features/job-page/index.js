import React, { useState, useContext, useRef } from 'react';
import { makeStyles, useTheme } from '@material-ui/core/styles';
import {
  Grid, Card, CardContent,
  InputLabel, FormControl, Select, Divider,
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

import ReactSpeedometer from "react-d3-speedometer"

import { PulseLoader } from "react-spinners";
import 'bootstrap/dist/css/bootstrap.min.css';
import 'react-bootstrap-table-next/dist/react-bootstrap-table2.min.css';

import { UserContext } from "../../app/user-context";

import "./index.css";
import { style } from '@mui/system';

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
  },
  dpicker: {
    color: 'theme.palette.text.primary',
  },
}));

export default function JobPage(props) {
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
    data_correction: false,
    error_action: 'Continue',
    job_name:'Test_PAT',
    user_name: user?.name,
    user_email: user?.email
  });

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
  const [uploadingJob, setUploadingJob] = useState(false);

  const [currentJob, setCurrentJob] = useState({job_id:0,status:'',finished:0})
  const [loadingCurrentJob, setLoadingCurrentJob] = useState(false);


  React.useEffect(() => {
    setLoadingServerList(true);

    const interval = setInterval(() => setLoadingCurrentJob(true), 10000);
    return () => {
      clearInterval(interval);
    };
  }, []);

  React.useEffect(() => {
    setParaString(JSON.stringify(jobParameter, null, '  '))
  }, [jobParameter]);


  //server list
  React.useEffect(() => {
    if (!loadingServerList) return;

    let lst = [];
    let js = localStorage.getItem('Server_List')
    if (js) lst = JSON.parse(js);

    let saved_svr = localStorage.getItem('currentServer');
    if (saved_svr) {
      lst.push(jobParameter.server)
      lst = [...new Set(lst)].sort();
      localStorage.setItem("Server_List", JSON.stringify(lst));
    }

    setJobParameter({ ...jobParameter, server: saved_svr, edm: '', rdm: '', portinfoid: 0, perilid: 0, analysisid: 0 });
    setServerList(lst);
    setDbList([]);
    setLoadingDbList(true);

    setLoadingServerList(false);

    // eslint-disable-next-line
  }, [loadingServerList]);

  //get EDM/RDM list 
  React.useEffect(() => {
    if (!loadingDbList) return;
    if (!jobParameter.server) {
      setLoadingDbList(false);
      return;
    }

    const request = '/api/dblist/' + jobParameter.server;
    fetch(request).then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(data => {
        setDbList(data);
        setJobParameter({ ...jobParameter, edm: data.edm[0], rdm: data.rdm[0], portinfoid: 0, perilid: 0, analysisid: 0 });
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
        if (data.length > 0) setJobParameter({ ...jobParameter, portinfoid: data[0].portinfoid })

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
        if (data.length > 0) setJobParameter({ ...jobParameter, perilid: data[0] })

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

    if (!jobParameter.server || !jobParameter.edm || jobParameter.portinfoid<=0){
      console.log("No input data!");
      return false;
    }

    if (jobParameter.perilid<=0){
      console.log("Need peril id!");
      return false;
    }

    if (jobParameter.subject_premium <=0){
      console.log("Need subject premium!");
      return false;
    }

    if (jobParameter.loss_alae_ratio <=0){
      console.log("Loss ALAE error!");
      return false;
    }

    if (!jobParameter.job_name){
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

    let form_data = new FormData();
    let para= jobParameter;
    para['job_guid'] = uuidv4();
    form_data.append('para',JSON.stringify(para));
    if(jobFile) form_data.append("data", jobFile);

    let request = 'api/Jobs'
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
            if(aid>0){
              setCurrentJob({job_id:aid,status:'',finished:0});
              setLoadingCurrentJob(true);
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
    // eslint-disable-next-line
  }, [uploadingJob]);

  //get current job status 
  let status_percent = {
    'received': 10,
    'data_extracted':40,
    'net_of_fac':60,
    'allocated':90,
    'finished':100
  }
  React.useEffect(() => {
    if (!loadingCurrentJob) return;
    if (!currentJob || currentJob.job_id <= 0) {
      setLoadingCurrentJob(false);
      return;
    }

    const request = '/api/Jobs/' + currentJob.job_id + '/Status';
    fetch(request).then(response => {
      if (response.ok) {
        return response.text();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(data => {
        if (data) {
          var perc= data in status_percent? status_percent[data]:0;
          setCurrentJob({...currentJob,status:data,finished:perc}) 
        }
        else{
          setCurrentJob({...currentJob,status:'',finished:0}) 
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
    if (longerLength == 0) {
      return 1.0;
    }
    return (longerLength - editDistance(longer, shorter)) / parseFloat(longerLength);
  };

  const editDistance = (s1, s2) => {
    s1 = s1.toLowerCase();
    s2 = s2.toLowerCase();

    var costs = new Array();
    for (var i = 0; i <= s1.length; i++) {
      var lastValue = i;
      for (var j = 0; j <= s2.length; j++) {
        if (i == 0)
          costs[j] = j;
        else {
          if (j > 0) {
            var newValue = costs[j - 1];
            if (s1.charAt(i - 1) != s2.charAt(j - 1))
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
                    {currentJob && currentJob.job_id>0?currentJob.job_id: ''}
                  </Typography>
                  <div className={classes.odometer} >
                    <ReactSpeedometer
                      width={280}
                      height={180}
                      marginTop={50}
                      marginBottom={50}
                      needleHeightRatio={0.7}
                      value={currentJob && currentJob.job_id>0? currentJob.finished: 0}
                      maxValue={100}
                      segments={4}
                      startColor="green"
                      endColor="blue"
                      needleColor="red"
                      needleTransitionDuration={2000}
                      needleTransition="easeElastic"
                      currentValueText={currentJob && currentJob.job_id>0? currentJob.status: 'Not yet submitted'}
                      currentValuePlaceholderStyle={'#{value}'}
                      textColor={theme.palette.text.primary}
                    />
                  </div>
                </Grid>
                <Grid md={4}>
                  <MenuList>
                    <MenuItem onClick={(e) => { 
                      if(ValidateJob()) setUploadingJob(true);
                    }}>Submit New Analysis</MenuItem>
                  </MenuList>
                </Grid>
              </Grid>
            </CardContent>
          </Card>
        </div>
        <textarea value={paraString}
          readOnly={true}
          class="job_bottom_row"
          style={{ color: theme.palette.text.primary, background: theme.palette.background.default }}>
        </textarea>
      </div>
      <div class="job_right_col">
        <div>
          <Accordion expanded
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

            </AccordionDetails>
          </Accordion>

          <Accordion expanded
            style={{ color: theme.palette.text.primary, background: theme.palette.background.default }}>
            <AccordionSummary
              expandIcon={<ExpandMoreIcon />}
              aria-controls="other-content"
              id="other-header"
            >
              <Typography>Allocation Options: </Typography>
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
                    <DatePicker className='dpicker'
                      label="Average Accident Date"
                      value={jobParameter.average_accident_date}
                      onChange={(newValue) => {
                        setJobParameter({ ...jobParameter, average_accident_date: newValue });
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
          <Accordion expanded
            style={{ color: theme.palette.text.primary, background: theme.palette.background.default }}>
            <AccordionSummary
              expandIcon={<ExpandMoreIcon />}
              aria-controls="corr-content"
              id="corr-header">
              <Typography>Data Correction</Typography>
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
                  <TextField id="alae-basic" label="Data Correction" variant="standard" readOnly
                    value={jobFile? jobFile.name: "No Correction"}
                  />
                </Box>
                <div class="row" style={{ alignItems: 'center' }} >
                    <div class="col-md-4 align-left vertical-align-top">
                        <Button variant="raised" component="label" className={classes.button}>
                          Add File
                          <input hidden
                              className={classes.input}
                              style={{ display: 'none' }}
                              id="raised-button-file"
                              type="file"
                              onChange={(e) => {
                                if (e.target.files && e.target.files.length > 0) {
                                  setJobFile(e.target.files[0]);
                                  setJobParameter({...jobParameter,data_correction:true})
                                }
                              }}
                            />
                        </Button>
                    </div>
                    <div class="col-md-4 align-left vertical-align-top">
                      <Button variant="raised" component="span" className={classes.button}
                        onClick={(e) => { setJobFile('');setJobParameter({...jobParameter,data_correction:false}) }}>
                        Remove File
                      </Button>
                    </div>
                  </div>
              </FormControl>
              <div>
              <FormControl className={classes.formControl}>
                <InputLabel shrink id="error-placeholder-label">
                  Error Action
                </InputLabel>
                <Select
                  labelId="error-placeholder-label"
                  id="error-placeholder"
                  value={jobParameter.error_action}
                  defaultValue={'Continue'}
                  onChange={event => {
                    if (jobParameter.error_action !== event.target.value) {
                      setJobParameter({ ...jobParameter, error_action: event.target.value });
                    }
                  }}
                >
                  {['Continue', 'Stop']
                    .map((n) => {
                      return <MenuItem value={n}>{ }{n}</MenuItem>
                    })}
                </Select>
              </FormControl>
              </div>
            </AccordionDetails>
          </Accordion>
          <Accordion expanded
            style={{ color: theme.palette.text.primary, background: theme.palette.background.default }}>
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