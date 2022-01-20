import React, { useState, useContext, useRef } from 'react';
import { useParams, useHistory } from "react-router-dom";
import { makeStyles, useTheme } from '@material-ui/core/styles';
import {
  Grid, Card, CardContent,
  InputLabel, FormControl, Select,
  Typography, Button, TextField, Box,
  MenuList, MenuItem, Divider
} from '@material-ui/core';

import BootstrapTable from 'react-bootstrap-table-next';
import paginationFactory from 'react-bootstrap-table2-paginator';
import ToolkitProvider, { Search } from 'react-bootstrap-table2-toolkit';
import 'bootstrap/dist/css/bootstrap.min.css';
import 'react-bootstrap-table-next/dist/react-bootstrap-table2.min.css';
import cellEditFactory from 'react-bootstrap-table2-editor';

import AdapterDateFns from '@mui/lab/AdapterDateFns';
import LocalizationProvider from '@mui/lab/LocalizationProvider';
import DatePicker from '@mui/lab/DatePicker';

import Tooltip from '@mui/material/Tooltip';

import Autocomplete from "@material-ui/lab/Autocomplete";

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

import { UserContext } from "../../app/user-context";

import "./index.css";
import ValidRules from "./valid-flag";

import { v4 as uuidv4 } from 'uuid';
import { psold_rg, blending_columns } from './blend';
import { psold_curves, fls_curves, mb_curves } from './curves';

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
  const [currentJob, setCurrentJob] = useState({ job_id: user ? user.curr_job : 0, status: '', finished: 0 })
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

        type_of_rating: "PSOLD",
        curve_id: 2016,
        average_accident_date: "1/1/2022",
        psold: {
          curve_persp: 'Gross',
          peril_subline: "All_Perils",
          trend_factor: 1.035,
          blending: psold_rg.map(w => w.weight),
          hpr_blending: false
        },
        // fls: {
        //   mu:0.0,
        //   w:0.0,
        //   tau:0.0,
        //   theta:0.0,
        //   beta:0.0,
        //   cap:0.0
        // },
        // mb: {
        //   b:0.0,
        //   g:0.0,
        // },
        loss_alae_ratio: 1,
        coverage_type: "Building_Contents_BI",
        additional_coverage: 2.0,
        deductible_treatment: "Retains_Limit",

        data_correction: "",
        valid_rules: 0,

        user_name: user?.name,
        user_email: user?.email,
      },
      data_file: null,
      use_ref: false
    })
  const [paraString, setParaString] = useState('');
  const [jobList, setJobList] = useState([]);
  const [selectedNewJob, setSelectedNewJob] = useState('');

  const [batchFile, setBatchFile] = useState('');
  const [readingBatch, setReadingBatch] = useState(false);

  const [inpExpanded, setInpExpanded] = useState(0x108);
  const [loadingServerList, setLoadingServerList] = useState(false);
  const [serverList, setServerList] = useState();
  const [currServer, setCurrServer] = useState('');

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

  const [curveList, setCurveList] = useState([])
  const [psoldBlending, setPsoldBlending] = useState([])
  const [savePsold, setSavePsold] = useState(
    {
      curve_persp: 'Gross',
      peril_subline: "All_Perils",
      trend_factor: 1.035,
      blending: psold_rg.map(w => w.weight),
      hpr_blending: false
      })
  const [saveFls, setSaveFls] = useState({ 
      mu:0.0,
      w:0.0,
      tau:0.0,
      theta:0.0,
      beta:0.0,
      cap:0.0
    })
  const [saveMb, setSaveMb] = useState({
      custom_type: 1, 
      b:0.0,
      g:0.0
    })

  React.useEffect(() => {
    setLoadingServerList(true);

    var lst = psold_rg.map(c => {
      return {
        id: c.id,
        name: c.name,
        weight: c.weight
      }
    });
    if (lst && lst.length > 0 && newJob && newJob.parameter && newJob.parameter.psold
      && newJob.parameter.psold.blending) {
      for (var i = 0; i < lst.length; i++) {
        lst[i].weight = newJob.parameter.psold.blending[i];
      }
      setPsoldBlending(lst)
    }

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
      lst.push(svr.toUpperCase())
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
          job['job_guid'] = '';
          job['user_name'] = user?.name;
          job['user_email'] = user?.email;
          job['data_correction'] = ''
          job['ref_analysis'] = 0;
          if (data.data_extracted > 0) {
            setRefJob({
              job_id: data.job_id,
              data_flag: flagRefJob(job)
            })
          };

          handleNewRateType(job.type_of_rating);
          if(job.type_of_rating=='PSOLD'){
            var lst1 = []
            if (job.psold && job.psold.blending) {
              for (var i = 0; i < psoldBlending.length; i++) {
                lst1[i] = {
                  id: psoldBlending[i].id,
                  name: psoldBlending[i].name,
                  weight: job.psold.blending[i]
                };
              }
            }
            else lst1 = psold_rg.map(c => {
              return {
                id: c.id,
                name: c.name,
                weight: c.weight
              }
            });
            setPsoldBlending(lst1)
          }
          setNewJob({ parameter: { ...newJob.parameter, ...job }, data_file: null, use_ref: true });

          if ('server' in job) svr = job['server'];
          if (svr) {
            lst.push(svr.toUpperCase())
            lst = [...new Set(lst)].sort();
            setServerList(lst)
            setCurrServer(svr);

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
      handleNewRateType(newJob.parameter.type_of_rating);
    }
    setLoadingServerList(false);

    // eslint-disable-next-line
  }, [loadingServerList]);


  React.useEffect(() => {
    if (newJob) {
      var js = newJob.parameter;
      if (refJob && newJob.use_ref && flagRefJob(js) === refJob.data_flag)
        js.ref_analysis = parseInt(refJob.job_id);
      else js.ref_analysis = 0;
      setParaString(JSON.stringify(js, null, '  '))
    }
    else setParaString('');
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

        var lst = serverList;
        lst.push(newJob.parameter.server.toUpperCase());
        lst = [...new Set(lst)].sort();
        setServerList(lst);
        localStorage.setItem("Server_List", JSON.stringify(lst));
        setCurrServer(newJob.parameter.server.toUpperCase())

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

    if(! job.type_of_rating || !job.curve_id)
    {
      console.log("Need rating curve!");
      return false;
    }

    if( job.type_of_rating=='PSOLD'){
      if(!job.psold){
        console.log("Need PSOLD parameter!");
        return false;
      };
      if(job.psold.blending){
         if(job.psold.blending.every(w=> w<=0)) {
           delete job.psold.blending;
         }
      }
      if (job.fls) delete job.fls;
      if (job.mb) delete job.mb;
    }
    else if( job.type_of_rating == 'FLS'){
      if(job.curve_id==57 && !job.fls){
          console.log("Need FLS parameter!");
          return false;
      }
      if (job.psold) delete job.psold;
      if (job.mb) delete job.mb;
    }
    else if( job.type_of_rating=='MB'){
      if(!job.mb){
        console.log("Need MB parameter!");
        return false;
      }
      else if(job.curve_id!=58){
        delete job.mb.b;
        delete job.mb.g;
      }

      if (job.psold) delete job.psold;
      if (job.fls) delete job.fls;
    }

    return true;
  };

  //submit job
  React.useEffect(() => {
    if (!uploadingJob) return;
    if (!jobList || jobList.length <= 0) {
      setUploadingJob(false);
      return;
    }

    jobList.forEach((job) => {
      if (ValidateJob(job.parameter)) {

        let form_data = new FormData();
        let js = job.parameter;
        if (job.data_file) {
          js['data_correction'] = job.data_file.name;
          form_data.append("data", job.data_file);
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

  const handleNewRateType = (rt) => {
    var lst = [];
    if (rt === "PSOLD") {
      if (newJob.parameter.coverage_type === 'Building_Only' || newJob.parameter.coverage_type === 'Contents_Only')
        lst = psold_curves.filter(c => c.ID === 2020);
      else lst = psold_curves;
    }
    else if (rt === "FLS") lst = fls_curves
    else if (rt === "MB") lst = mb_curves

    setCurveList(lst);
  };

  const handleConfirm = (isOK) => {
    var it = confirm
    setConfirm('');
    if (isOK) {
      if (it === "stop the current analysis") handleStopJob();
      else if (it === "start/rerun the current analysis") handleStartJob();
      else if (it === "submit jobs") setUploadingJob(true);
      else if (it === "delete the SELECTED job") handleDelJob(false);
      else if (it === "delete ALL jobs in the list") handleDelJob(true);
      else if (it === "update the selected job") handleUpdateJob();
    }
  };

  const handleUpdateJob = () => {
    var lst = jobList;
    if (lst) {
      var sel = lst.find(j => j.parameter['job_guid'] === selectedNewJob);
      if (sel && ValidateJob(sel.parameter)) {
        sel.parameter = { ...newJob.parameter, job_guid: selectedNewJob }
        sel.data_file = newJob.data_file;
        setJobList(lst);
      }
      else alert("Job update parameters failed!");
    }
  };

  const handleDelJob = (all = false) => {
    var lst = jobList;
    if (all) {
      setJobList([]);
    }
    else {
      if (lst) {
        var sel = lst.find(j => j.parameter['job_guid'] === selectedNewJob);
        if (sel) {
          lst.pop(sel);
          setJobList(lst);
        }
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

  React.useEffect(() => {
    if (!setReadingBatch) return;
    if (!batchFile) {
      setReadingBatch(false);
      return;
    }

    var fr = new FileReader();
    fr.onload = function () {
      var lns = fr.result.split(/\r?\n/g);
      var head = lns[0].split(',');
      var n = head.length;

      var lst = jobList;
      if (!lst) lst = []

      lns.slice(1).forEach(ln => {
        var col = ln.split(',');
        if (col.length === n) {

          var job = {
            parameter: JSON.parse(JSON.stringify(newJob.parameter)),
            data_file: newJob.data_file,
            use_ref: newJob.use_ref
          };
          job.parameter['job_guid'] = uuidv4();

          for (var i = 0; i < n; i++) {
            if (typeof (job.parameter[head[i]]) === 'number') job.parameter[head[i]] = Number(col[i]);
            else job.parameter[head[i]] = col[i];
          }

          if (refJob && job.use_ref && flagRefJob(job.parameter) === refJob.data_flag)
            job.parameter.ref_analysis = parseInt(refJob.job_id);
          else job.parameter.ref_analysis = 0;

          lst.push(job);
        }
      });

      setJobList(lst);
      setBatchFile('');
      setReadingBatch(false);
    };
    fr.readAsText(batchFile);
    // eslint-disable-next-line
  }, [readingBatch]);

  const flagRefJob = (job) => {
    if (job) return job.server
      + "|" + job.edm + "|" + job.rdm + "|" + job.portinfoid
      + "|" + job.perilid + "|" + job.analysisid
      + "|" + job.additional_coverage + "|" + job.deductible_treatment
    else return null
  };

  const pulseLoading = () => {
    return loadingServerList || loadingPortList || loadingPerilList || loadingDbList || uploadingJob || loadingCurrentJob || readingBatch;
  };

  return (
    <div class="job_container">
      {pulseLoading() &&
        <div className={classes.spinner}>
          <PulseLoader
            size={30}
            color={"#2BAD60"}
            loading={pulseLoading()}
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
        <div class='row' style={{ marginLeft: '2px' }}>
          <Tooltip title="Add job to list (Hold Ctrl key to add with batch)">
            <Button style={{ outline: 'none', height: '36px' }}
              onClick={(e) => {
                if (e.ctrlKey) inputFile.current.click();
                else {
                  var lst = jobList;
                  if (!lst) lst = []

                  var job = {
                    parameter: JSON.parse(JSON.stringify(newJob.parameter)),
                    data_file: newJob.data_file,
                    use_ref: newJob.use_ref
                  };
                  if (ValidateJob(job.parameter)) {
                    job.parameter['job_guid'] = uuidv4();
                    if (refJob && job.use_ref && flagRefJob(job.parameter) === refJob.data_flag)
                      job.parameter.ref_analysis = parseInt(refJob.job_id);
                    else job.parameter.ref_analysis = 0;

                    lst.push(job);
                    setJobList(lst);
                    setSelectedNewJob(job.parameter.job_guid);
                    setNewJob({ ...newJob, parameter: { ...job.parameter, job_guid: '' } });
                  }
                  else alert("Job parameter error!")
                }
              }}
            >Add to List
            </Button>
          </Tooltip>
          <input type='file' id='file' ref={inputFile} style={{ display: 'none' }} onChange={(e) => {
            if (e.target.files && e.target.files.length > 0) {
              setBatchFile(e.target.files[0]);
              setReadingBatch(true);
            }
          }} />
          <Tooltip title="Remove selected job from list (Hold Ctrl key to remove all)">
            <Button style={{ outline: 'none', height: '36px' }}
              disabled={!jobList || jobList.length <= 0 || !selectedNewJob || !jobList.find(j => j.parameter['job_guid'] === selectedNewJob)}
              onClick={(e) => {
                if (e.ctrlKey) setConfirm("delete ALL jobs in the list");
                else setConfirm("delete the SELECTED job");
              }}
            >Remove
            </Button>
          </Tooltip>
          <Tooltip title="Update selected job with the new settings">
            <Button style={{ outline: 'none', height: '36px' }}
              disabled={!jobList || jobList.length <= 0 || !selectedNewJob || !jobList.find(j => j.parameter['job_guid'] === selectedNewJob)}
              onClick={() => {
                setConfirm("update the selected job");
              }}
            >Update
            </Button>
          </Tooltip>
          <Divider orientation="vertical" flexItem />
          <Tooltip title="Submit jobs in the list to backend service">
            <Button style={{ outline: 'none', height: '36px' }}
              disabled={!jobList || jobList.length <= 0}
              onClick={() => {
                setConfirm("submit jobs");
              }}
            >Submit All
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
                var lst = jobList;
                if (lst) {
                  var sel = lst.find(j => j.parameter['job_guid'] === event.target.value);
                  if (sel) {
                    var para = JSON.parse(JSON.stringify(sel.parameter));
                    para['job_guid'] = '';

                    setNewJob({ parameter: para, use_ref: sel.use_ref, data_file: sel.data_file });
                    setSelectedNewJob(event.target.value);
                    handleNewRateType(para.type_of_rating);
                    if (para.type_of_rating=='PSOLD' && para.psold && para.psold.blending) {
                      var lst1 = []
                      var wgts = psoldBlending;
                      for (var i = 0; i < wgts.length; i++) {
                        lst1[i] = {
                          id: wgts[i].id,
                          name: wgts[i].name,
                          weight: para.psold.blending[i]
                        };
                      }
                      setPsoldBlending(lst1)
                    }
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
              <TextField id="alae-basic" label="New Job Name" variant="standard"
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
            <FormControl className={classes.formControl}>
              <InputLabel shrink id="coverage-placeholder-label">
                Coverage Typs
              </InputLabel>
              <Select
                labelId="coverage-placeholder-label"
                id="coverage-placeholder"
                value={newJob.parameter.coverage_type}
                defaultValue={'Building_Contents_BI'}
                onChange={event => {
                  if (newJob.parameter.coverage_type !== event.target.value) {
                    var para = newJob.parameter;
                    para.coverage_type = event.target.value;
                    if ((event.target.value === 'Building_Only' || event.target.value === 'Contents_Only')
                      && para.type_of_rating === "PSOLD") para.curve_id = 2020;
                    setNewJob({ ...newJob, parameter: para });
                    handleNewRateType(para.type_of_rating)
                  }
                }}
              >
                {['Building_Contents_BI', 'Building_Contents', 'Building_Only', 'Contents_Only'].map((n) => {
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

            <FormControl className={classes.formControl}>
              <InputLabel shrink id="deductible-placeholder-label">
                Deductible Type
              </InputLabel>
              <Select
                labelId="deductible-placeholder-label"
                id="deductible-placeholder"
                value={newJob.parameter.deductible_treatment}
                defaultValue={'Retains_Limit'}
                onChange={event => {
                  if (newJob.parameter.deductible_treatment !== event.target.value) {
                    setNewJob({ ...newJob, parameter: { ...newJob.parameter, deductible_treatment: event.target.value } });
                  }
                }}
              >
                {['Retains_Limit', 'Erodes_Limit']
                  .map((n) => {
                    return <MenuItem value={n}>{ }{n}</MenuItem>
                  })}
              </Select>
            </FormControl>

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
          </div>
          <div>
            <FormControl className={classes.formControl}
              hidden={!newJob || !newJob.parameter || !refJob ||
                flagRefJob(newJob?.parameter) !== refJob.data_flag}
            >
              <FormControlLabel control={
                <Checkbox style={{ color: theme.palette.text.primary, background: theme.palette.background.default }}
                  checked={newJob && newJob.use_ref && newJob.parameter && refJob &&
                    flagRefJob(newJob?.parameter) === refJob.data_flag}
                  onChange={event => {
                    setNewJob({ ...newJob, use_ref: event.target.checked });
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
              <Typography>Policy/Location/Fac Input: </Typography>
            </AccordionSummary>
            <AccordionDetails>
              <div>
                <Autocomplete className={classes.formControl}
                  options={serverList}
                  inputValue={currServer}
                  noOptionsText="Enter to add a server"
                  onInputChange={(event, newValue) => {
                    if (!event && newValue === '') setCurrServer(newJob.parameter.server);
                    else setCurrServer(newValue);

                    if (event && (event.type === 'blur' || event.type === 'click' || (event.type === 'keydown' && event.key === 'Enter'))
                      && newValue !== newJob.parameter.server) {
                      setNewJob({ ...newJob, parameter: { ...newJob.parameter, server: newValue, rdm: '', edm: '', portinfoid: 0, analysisid: 0 } });
                      setLoadingDbList(true);
                    }
                  }}
                  renderInput={(params) => (
                    <TextField
                      {...params}
                      defaultValue={''}
                      label="Server"
                      // variant="outlined"
                      onKeyDown={(e) => {
                        if (
                          e.key === "Enter" && currServer && currServer.length > 1 &&
                          serverList.findIndex((o) => o === currServer) === -1
                        ) {
                          var lst = serverList;
                          var svr = currServer.toUpperCase();
                          lst.push(svr);
                          lst = [...new Set(lst)].sort();
                          setServerList(lst);
                        }
                      }}
                    />
                  )}
                />
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
              <Typography>Rating Type:</Typography>
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
                        var job = newJob.parameter;                        
                        if(job['type_of_rating'] === 'PSOLD'){
                          setSavePsold({...job.psold});
                        }
                        else if (job['type_of_rating']==='FLS' && job.curve_id === 57) {
                          setSaveFls({...job.fls});
                        }
                        else if (job['type_of_rating']==='MB' && job.curve_id === 58) {
                          setSaveMb({...job.mb});
                        }
                        job.curve_id =1;
                        if (event.target.value === "PSOLD") {
                          if (newJob.parameter.coverage_type === 'Building_Only' 
                            || newJob.parameter.coverage_type === 'Contents_Only') 
                            job.curve_id = 2020;
                          else job.curve_id = 2016;
                       
                          job['psold']={...savePsold};
                          if (job.fls) delete job.fls;
                          if (job.mb) delete job.mb;
                        }
 
                        job['type_of_rating'] = event.target.value
                        setNewJob({ ...newJob, parameter: {...job} });
                        handleNewRateType(event.target.value);
                      }
                    }}
                  >
                    {['PSOLD', 'FLS', 'MB'].map((n) => {
                      return <MenuItem value={n}>{ }{n}</MenuItem>
                    })}
                  </Select>
                </FormControl>
                <FormControl className={classes.formControl}>
                  <InputLabel shrink id="ct-placeholder-label">
                    Curve ID
                  </InputLabel>
                  <Select
                    labelId="ct-placeholder-label"
                    id="ct-placeholder"
                    value={newJob.parameter.curve_id}
                    onChange={event => {
                      if (newJob.parameter.curve_id !== event.target.value) {
                        var job = newJob.parameter;
                        job['curve_id']= event.target.value;
                        if(job['type_of_rating']==='PSOLD'){
                          job['psold']={...savePsold};
                          if (job.fls) delete job.fls;
                          if (job.mb) delete job.mb;
                        }
                        else if (job['type_of_rating']==='FLS'){
                          if(event.target.value == 57)  job['fls'] = {...saveFls};
                          if (job.psold) delete job.psold;
                          if (job.mb) delete job.mb;
                        }
                        else if (job['type_of_rating']==='MB') {
                          if(event.target.value == 58) job['mb'] ={...saveMb};
                          if (job.fls) delete job.fls;
                          if (job.psold) delete job.psold;
                        }
                        setNewJob({ ...newJob, parameter: {...job} });
                      }
                    }}
                  >
                    {curveList.map((n) => {
                      return <MenuItem value={n.ID}>{ }{ newJob.parameter.type_of_rating==='PSOLD'? n.name: n.ID + ' -- ' + n.name}</MenuItem>
                    })}
                  </Select>
                </FormControl>
              </div>
              <div id='psold_para' hidden={newJob.parameter.type_of_rating !== 'PSOLD'}>
                <div>
                  <FormControl className={classes.formControl}>
                    <InputLabel shrink id="curve-placeholder-label">
                      Curve Type
                    </InputLabel>
                    <Select
                      labelId="curve-placeholder-label"
                      id="curve-placeholder"
                      value={newJob.parameter.psold?.curve_persp}
                      defaultValue={'Gross'}
                      onChange={event => {
                        if (newJob.parameter.psold?.curve_persp !== event.target.value) {
                          setNewJob({ ...newJob, parameter: { ...newJob.parameter, psold: { ...newJob.parameter.psold, curve_persp: event.target.value } } });
                        }
                      }}
                    >
                      {['Gross', 'Net']
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
                      value={newJob.parameter.psold?.peril_subline}
                      defaultValue={'All_Perils'}
                      onChange={event => {
                        if (newJob.parameter.psold?.peril_subline !== event.target.value) {
                          setNewJob({ ...newJob, parameter: { ...newJob.parameter, psold: { ...newJob.parameter.psold, peril_subline: event.target.value } } });
                        }
                      }}
                    >
                      {['Fire', 'Wind', 'Special_Causes', 'All_Perils']
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
                      <TextField id="trend-basic" label="Trend Factor" variant="standard" type='number'
                        value={newJob.parameter.psold?.trend_factor}
                        inputProps={{
                          maxLength: 13,
                          step: "0.01"
                        }}
                        onChange={event => {
                          if (newJob.parameter.psold?.trend_factor !== event.target.value) {
                            setNewJob({ ...newJob, parameter: { ...newJob.parameter, psold: { ...newJob.parameter.psold, trend_factor: parseFloat(event.target.value) } } });
                          }
                        }}
                      />
                    </Box>
                  </FormControl>
                </div>
                <Accordion style={{ color: theme.palette.text.primary, background: theme.palette.background.default }}
                  expanded={inpExpanded & 0x100}
                  onChange={(event, isExpanded) => {
                    if (isExpanded) setInpExpanded(inpExpanded | 0x100);
                    else setInpExpanded(inpExpanded & ~0x100);
                  }}>
                  <AccordionSummary
                    expandIcon={<ExpandMoreIcon />}
                    aria-controls="corr-content"
                    id="corr-header">
                    <Typography>Blending Weights:</Typography>
                  </AccordionSummary>
                  <AccordionDetails>
                    <FormControl className={classes.formControl}>
                      <ToolkitProvider
                        keyField="id"
                        data={psoldBlending}
                        columns={blending_columns}
                        bootstrap4
                      >
                        {
                          props => (
                            <div justify='flex-end'>
                              <BootstrapTable classes={classes.table}
                                cellEdit={cellEditFactory({
                                  mode: 'click', blurToSave: true, afterSaveCell: (oldValue, newValue, row, column) => {
                                    var job = newJob.parameter;
                                    job['psold']['blending'] = psoldBlending.map(w => w.weight)
                                    setNewJob({ ...newJob, parameter: {...job} });
                                  }
                                })}
                                {...props.baseProps}
                                rowClasses={classes.table_row}
                                striped
                                hover
                                condensed
                                pagination={paginationFactory({
                                  sizePerPage: 10,
                                  hideSizePerPage: true,
                                  hidePageListOnlyOnePage: true,
                                })}
                              />
                            </div>
                          )
                        }
                      </ToolkitProvider>
                    </FormControl>
                  </AccordionDetails>
                </Accordion>
                <FormControl className={classes.formControl}>
                  <FormControlLabel control={
                    <Checkbox style={{ color: theme.palette.text.primary, background: theme.palette.background.default }}
                      checked={newJob && newJob.parameter && newJob.parameter.psold?.hpr_blending}
                      onChange={event => {
                        setNewJob({ ...newJob, parameter: { ...newJob.parameter, psold: { ...newJob.parameter.psold, hpr_blending: event.target.checked } } });
                      }}
                    />} label={"Use HPR blending"}
                  />
                </FormControl>
              </div>

              <div id='fls_para' hidden={newJob.parameter.type_of_rating !== 'FLS'  || !newJob.parameter.fls
                  || newJob.parameter.curve_id !== 57}>
                <FormControl className={classes.formControl}>
                    <Box component="form" sx={{'& > :not(style)': { m: 1, width: '18ch' },}}
                      noValidate
                      autoComplete="off"
                    >
                      <TextField label="mu" variant="standard" type='number'
                        value={newJob.parameter.fls?.mu}
                        inputProps={{maxLength: 13,step: "0.01"}}
                        onChange={event => {
                          if (newJob.parameter.fls?.mu !== event.target.value) {
                            setNewJob({ ...newJob, parameter: { ...newJob.parameter, fls: { ...newJob.parameter.fls, mu: parseFloat(event.target.value) } } });
                          }
                        }}
                      />
                    </Box>
                  </FormControl> 
                   <FormControl className={classes.formControl}>
                    <Box component="form" sx={{'& > :not(style)': { m: 1, width: '18ch' },}}
                      noValidate
                      autoComplete="off"
                    >
                      <TextField label="w" variant="standard" type='number'
                        value={newJob.parameter.fls?.w}
                        inputProps={{maxLength: 13,step: "0.1"}}
                        onChange={event => {
                          if (newJob.parameter.fls?.w !== event.target.value) {
                            setNewJob({ ...newJob, parameter: { ...newJob.parameter, fls: { ...newJob.parameter.fls, w: parseFloat(event.target.value) } } });
                          }
                        }}
                      />
                    </Box>
                  </FormControl> 
                  <FormControl className={classes.formControl}>
                    <Box component="form" sx={{'& > :not(style)': { m: 1, width: '18ch' },}}
                      noValidate
                      autoComplete="off"
                    >
                      <TextField label="tau" variant="standard" type='number'
                        value={newJob.parameter.fls?.tau}
                        inputProps={{maxLength: 13,step: "0.1"}}
                        onChange={event => {
                          if (newJob.parameter.fls?.tau !== event.target.value) {
                            setNewJob({ ...newJob, parameter: { ...newJob.parameter, fls: { ...newJob.parameter.fls, tau: parseFloat(event.target.value) } } });
                          }
                        }}
                      />
                    </Box>
                  </FormControl>
                  <FormControl className={classes.formControl}>
                    <Box component="form" sx={{'& > :not(style)': { m: 1, width: '18ch' },}}
                      noValidate
                      autoComplete="off"
                    >
                      <TextField label="theta" variant="standard" type='number'
                        value={newJob.parameter.fls?.theta}
                        inputProps={{maxLength: 13,step: "0.01"}}
                        onChange={event => {
                          if (newJob.parameter.fls?.theta !== event.target.value) {
                            setNewJob({ ...newJob, parameter: { ...newJob.parameter, fls: { ...newJob.parameter.fls, theta: parseFloat(event.target.value) } } });
                          }
                        }}
                      />
                    </Box>
                  </FormControl> 
                  <FormControl className={classes.formControl}>
                    <Box component="form" sx={{'& > :not(style)': { m: 1, width: '18ch' },}}
                      noValidate
                      autoComplete="off"
                    >
                      <TextField label="beta" variant="standard" type='number'
                        value={newJob.parameter.fls?.beta}
                        inputProps={{maxLength: 13,step: "0.01"}}
                        onChange={event => {
                          if (newJob.parameter.fls?.beta !== event.target.value) {
                            setNewJob({ ...newJob, parameter: { ...newJob.parameter, fls: { ...newJob.parameter.fls, beta: parseFloat(event.target.value) } } });
                          }
                        }}
                      />
                    </Box>
                  </FormControl> 
                  <FormControl className={classes.formControl}>
                    <Box component="form" sx={{'& > :not(style)': { m: 1, width: '18ch' },}}
                      noValidate
                      autoComplete="off"
                    >
                      <TextField label="cap" variant="standard" type='number'
                        value={newJob.parameter.fls?.cap}
                        inputProps={{maxLength: 13,step: "0.1"}}
                        onChange={event => {
                          if (newJob.parameter.fls?.cap !== event.target.value) {
                            setNewJob({ ...newJob, parameter: { ...newJob.parameter, fls: { ...newJob.parameter.fls, cap: parseFloat(event.target.value) } } });
                          }
                        }}
                      />
                    </Box>
                  </FormControl> 
              </div>

              <div id='mb_para' hidden={newJob.parameter.type_of_rating !== 'MB'}>
                <FormControl className={classes.formControl}>
                  <InputLabel shrink id="mb-placeholder-label">
                    Custom Type
                  </InputLabel>
                  <Select
                    labelId="mb-placeholder-label"
                    id="mb-placeholder"
                    value={newJob.parameter.mb?.custom_type}
                    defaultValue={1}
                    onChange={event => {
                      if (newJob.parameter.mb?.custom_type !== event.target.value) {
                        if (newJob.parameter.mb?.custom_type !== event.target.value) {
                          setNewJob({ ...newJob, parameter: { ...newJob.parameter, mb: { ...newJob.parameter.mb, custom_type: parseInt(event.target.value) } } });
                        }

                      }
                    }}
                  >
                    <MenuItem value={1}>{ }Custom Type #1: </MenuItem>
                    <MenuItem value={2}>{ }Custom Type #2: </MenuItem>
                    <MenuItem value={3}>{ }Custom Type #3: </MenuItem>
                  </Select>
                </FormControl>
                <div id='mb_para1' hidden={newJob.parameter.type_of_rating !== 'MB' || !newJob.parameter.mb
                      || newJob.parameter.curve_id !== 58}>
                  <FormControl className={classes.formControl}>
                    <Box component="form" sx={{'& > :not(style)': { m: 1, width: '20ch' },}}
                      noValidate
                      autoComplete="off"
                    >
                      <TextField label="b" variant="standard" type='number'
                        value={newJob.parameter.mb?.b}
                        inputProps={{maxLength: 13,step: "0.01"}}
                        onChange={event => {
                          if (newJob.parameter.mb?.b !== event.target.value) {
                            setNewJob({ ...newJob, parameter: { ...newJob.parameter, mb: { ...newJob.parameter.mb, b: parseFloat(event.target.value) } } });
                          }
                        }}
                      />
                    </Box>
                  </FormControl> 
                  <FormControl className={classes.formControl}>
                    <Box component="form" sx={{'& > :not(style)': { m: 1, width: '20ch' },}}
                      noValidate
                      autoComplete="off"
                    >
                      <TextField label="g" variant="standard" type='number'
                        value={newJob.parameter.mb?.g}
                        inputProps={{maxLength: 13,step: "1"}}
                        onChange={event => {
                          if (newJob.parameter.mb?.g !== event.target.value) {
                            setNewJob({ ...newJob, parameter: { ...newJob.parameter, mb: { ...newJob.parameter.mb, g: parseFloat(event.target.value) } } });
                          }
                        }}
                      />
                    </Box>
                  </FormControl> 
                </div>
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
              <Typography>Validation Rules:</Typography>
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
                    value={(newJob.data_file ? newJob.data_file.name : "No Correction")}
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
                      onClick={(e) => { setNewJob({ ...newJob, data_file: null }) }}>
                      Remove
                    </Button>
                  </div>
                </div>
              </FormControl>
              <div>
                <ul style={{ listStyleType: "none" }}>
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
              <Typography>Job Summary:</Typography>
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