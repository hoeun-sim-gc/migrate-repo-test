import React, { useState, useContext } from 'react';
import { makeStyles, useTheme } from '@material-ui/core/styles';
import {
  Card, CardContent,
  InputLabel, FormControl, Select,
  Typography, Button, TextField, Box,
  MenuItem, Divider
} from '@material-ui/core';

import BootstrapTable from 'react-bootstrap-table-next';
import paginationFactory from 'react-bootstrap-table2-paginator';
import ToolkitProvider from 'react-bootstrap-table2-toolkit';
import 'bootstrap/dist/css/bootstrap.min.css';
import 'react-bootstrap-table-next/dist/react-bootstrap-table2.min.css';
import cellEditFactory from 'react-bootstrap-table2-editor';

import AdapterDateFns from '@mui/lab/AdapterDateFns';
import LocalizationProvider from '@mui/lab/LocalizationProvider';
import DatePicker from '@mui/lab/DatePicker';

import Tooltip from '@mui/material/Tooltip';
import Stepper from '@mui/material/Stepper';
import Step from '@mui/material/Step';
import StepButton from '@mui/material/StepButton';

import Autocomplete from "@material-ui/lab/Autocomplete";

import {
  Checkbox, FormControlLabel,
  Dialog, DialogActions, DialogContent, DialogContentText, DialogTitle
} from '@mui/material';



import { PulseLoader } from "react-spinners";

import { UserContext } from "../../app/user-context";

import "./index.css";
import ValidRules from "./valid-flag";

import { v4 as uuidv4 } from 'uuid';
import { psold_rg, blending_columns, blending_types } from './blend';
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
    margin: 10,
    textAlign: 'left'
  },
  c_label: {
    marginTop: '15px',
    marginBottom: '10px',
    marginLeft: '5px'
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
  const classes = useStyles();
  const theme = useTheme();
  const [user,] = useContext(UserContext);

  const steps = ['Policy Data', 'Rating Type', 'Other Settings'];
  const [activeStep, setActiveStep] = useState(0);

  const [refJob, setRefJob] = useState({
    job_id: 0,
    cat_data: {
      server: "",
      edm: "",
      rdm: "",
      portinfoid: 0,
      perilid: 0,
      analysisid: 0
    }
  })

  const [newJob, setNewJob] = useState(
    {
      parameter: {
        job_name: "PAT_Test",
        job_guid: "",

        data_source_type: "Cat_Data",
        cat_data:
        {
          server: "",
          edm: "",
          rdm: "",
          portinfoid: 0,
          perilid: 0,
          analysisid: 0
        },
        // reference_job:
        // {
        //   job_id : 12345 
        // }
        data_correction: "",
        valid_rules: 0,

        type_of_rating: "PSOLD",
        curve_id: 2016,
        psold: {
          curve_persp: 'Gross',
          peril_subline: "All_Perils",
          trend_factor: 1.035,
          average_accident_date: "1/1/2022",
          blending_type: 'no_blending',
          blending_weights: psold_rg.map(w => w.weight),
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

        user_name: user?.name,
        user_email: user?.email,
      },
      data_file: null
    })
  const [paraString, setParaString] = useState('');

  const [dataSrcList, setDataSrcList] = useState(['User_Upload', 'Cat_Data']);

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

  const [submitingJob, setSubmitingJob] = useState(false);
  const [submitingJobRun, setSubmitingJobRun] = useState(false);
  const [confirm, setConfirm] = React.useState(false);

  const [curveList, setCurveList] = useState([])
  const [psoldBlending, setPsoldBlending] = useState([])
  const [savePsold, setSavePsold] = useState(
    {
      curve_persp: 'Gross',
      peril_subline: "All_Perils",
      trend_factor: 1.035,
      blending_type: 'no_blending',
      blending_weights: psold_rg.map(w => w.weight),
      hpr_blending: false
    })
  const [saveFls, setSaveFls] = useState({
    mu: 0.0,
    w: 0.0,
    tau: 0.0,
    theta: 0.0,
    beta: 0.0,
    cap: 0.0
  })
  const [saveMb, setSaveMb] = useState({
    custom_type: 1,
    b: 0.0,
    g: 0.0
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
    setPsoldBlending(lst)
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
    if (user?.curr_job && user.curr_job > 0) {
      const request = '/api/job/' + user.curr_job;
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
          var r = refJob;
          var dt = job.data_source_type;

          if (data.data_extracted > 0) {
            r.job_id = data.job_id;
            setDataSrcList(['Reference_Job', 'User_Upload', 'Cat_Data']);
            dt = 'Reference_Job'
            if (job.cat_data) {
              r.cat_data = job.cat_data;
            }
          }

          setRefJob({ ...r });
          job = prepareDataChange(job, dt);

          handleNewRateType(job.type_of_rating);
          if (job.type_of_rating === 'PSOLD') {
            var lst1 = []
            if (job.psold && job.psold.blending_type !== 'no_blending') {
              for (var i = 0; i < psold_rg.length; i++) {
                lst1[i] = {
                  id: psold_rg[i].id,
                  name: psold_rg[i].name,
                  weight: job.psold.blending_weights[i]
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
          handleUpdateJob({ ...newJob, parameter: { ...job }, data_file: null });

          if (r.cat_data) svr = r.cat_data.server;
          if (svr) {
            lst.push(svr.toUpperCase())
            lst = [...new Set(lst)].sort();
            setServerList(lst)
            setCurrServer(svr);
            setDbList([]);

            //try to connect to SQL silently to sve time later
            const request = '/api/db_list/' + svr;
            fetch(request).then(response => {
              if (response.ok) {
                return response.json();
              }
              throw new TypeError("Oops, we haven't got data!");
            })
              .then(data => {
                if (data && data.rdm) data.rdm = [''].concat(data.rdm);
                setDbList(data);
              })
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
      handleNewRateType(newJob.parameter.type_of_rating);
      setLoadingServerList(false);
    }
    // eslint-disable-next-line
  }, [loadingServerList]);

  React.useEffect(() => {
    var para = { ...newJob.parameter };
    if (para) {

      var sort = ["job_name",
        "data_source_type", "cat_data", "reference_job",
        "data_correction", "valid_rules",
        "type_of_rating", "curve_id", "psold", "FLS", "mb",
        "user_email", "user_name"
      ];

      var total = Object.keys(para);
      var s1 = sort.filter(k => total.includes(k));
      var s2 = total.filter(k => !s1.includes(k)).sort();

      var u = s1.pop()
      if (u === "user_name") {
        s2.push(u);
        u = s1.pop()
        if (u === "user_email") s2.push(u);
      }
      else s1.appen(u);

      var sorted = {}
      s1.concat(s2).forEach(k => {
        if (para[k]) sorted[k] = para[k];
      });

      setParaString(JSON.stringify(sorted, (k, v) => {
        if (k === 'blending_weights') return v.join();
        else return v;
      }, 4));
    }
    else setParaString('');
  }, [newJob.parameter]);


  //get EDM/RDM list 
  React.useEffect(() => {
    if (!loadingDbList) return;
    if (!newJob.parameter.cat_data?.server) {
      setLoadingDbList(false);
      return;
    }

    const request = '/api/db_list/' + newJob.parameter.cat_data.server;
    fetch(request).then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(data => {
        if (data && data.rdm) data.rdm = [''].concat(data.rdm);
        setDbList(data);
        if (!data.edm.includes(newJob.parameter.cat_data.edm)) {
          handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, edm: data.edm[0], portinfoid: 0, perilid: 0 } });
        }
        if (!data.rdm.includes(newJob.parameter.cat_data.rdm)) {
          handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, rdm: data.rdm[0], analysisid: 0 } });
        }

        var lst = serverList;
        lst.push(newJob.parameter.cat_data.server.toUpperCase());
        lst = [...new Set(lst)].sort();
        setServerList(lst);
        localStorage.setItem("Server_List", JSON.stringify(lst));
        setCurrServer(newJob.parameter.cat_data.server.toUpperCase())

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
  }, [loadingDbList, newJob.parameter.cat_data?.server]);

  //get port list 
  React.useEffect(() => {
    if (!loadingPortList) return;
    if (!newJob.parameter.cat_data?.edm) {
      setLoadingPortList(false);
      return;
    }

    const request = '/api/port/' + newJob.parameter.cat_data.server + '/' + newJob.parameter.cat_data.edm;
    fetch(request).then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(data => {
        setPortList(data);
        if (data.length > 0) {
          if (data.filter(e => e.portinfoid === newJob.parameter.cat_data.portinfoid).length <= 0) {
            handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, portinfoid: data[0].portinfoid, perilid: 0 } })
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
  }, [loadingPortList, newJob.parameter.cat_data?.edm]);

  //get Peril list 
  React.useEffect(() => {
    if (!loadingPerilList) return;
    if (!newJob.parameter.cat_data?.edm || newJob.parameter.cat_data?.portinfoid <= 0) {
      setLoadingPerilList(false);
      return;
    }

    const request = '/api/peril/' + newJob.parameter.cat_data.server + '/' + newJob.parameter.cat_data.edm + '/' + newJob.parameter.cat_data.portinfoid;
    fetch(request).then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(data => {
        setPerilList(data);
        if (data.length > 0) {
          if (!data.includes(newJob.parameter.cat_data.perilid)) handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, perilid: data[0] } })
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
  }, [loadingPerilList, newJob.parameter.cat_data?.portinfoid]);


  //get analyis list 
  React.useEffect(() => {
    if (!loadingAnlsList) return;
    if (!newJob.parameter.cat_data?.rdm) {
      setLoadingAnlsList(false);
      return;
    }

    const request = '/api/anls/' + newJob.parameter.cat_data.server + '/' + newJob.parameter.cat_data.rdm;
    fetch(request).then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(data => {
        if (data) data = [{ id: 0, name: '' }].concat(data);
        else data = [{ id: 0, name: '' }]

        if (data.filter(r => r.id === newJob.parameter.cat_data.analysisid).length <= 0) {
          handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, analysisid: 0 } })
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
  }, [loadingAnlsList, newJob.parameter.cat_data?.rdm]);

  const ValidateJob = (job) => {
    if (!job) return false;

    if (job.data_source_type === 'Cat_data') {
      if (!job.server || !job.edm || job.portinfoid <= 0) {
        console.log("No input data!");
        return false;
      }
      if (job.perilid <= 0) {
        console.log("Need peril id!");
        return false;
      }
    }
    else if (job.data_source_type === 'Reference_Job') {
      if (!job.reference_job || job.reference_job.job_id <= 0) {
        console.log("No reference job id!");
        return false;
      }
    }

    if (job.loss_alae_ratio <= 0) {
      console.log("Loss ALAE error!");
      return false;
    }

    if (!job.job_name) {
      console.log("Need job name!");
      return false;
    }

    if (!job.type_of_rating || !job.curve_id) {
      console.log("Need rating curve!");
      return false;
    }

    if (job.type_of_rating === 'PSOLD') {
      if (!job.psold) {
        console.log("Need PSOLD parameter!");
        return false;
      };
      if ( job.psold.blending_type === 'no_blending' || 
          (job.psold.blending_weights && job.psold.blending_weights.every(w => w <= 0))) {
          job.psold.blending_type = 'no_blending';
          delete job.psold.hpr_blending;
          delete job.psold.blending_weights;
      }
      if (job.fls) delete job.fls;
      if (job.mb) delete job.mb;
    }
    else if (job.type_of_rating === 'FLS') {
      if (job.curve_id === 57 && !job.fls) {
        console.log("Need FLS parameter!");
        return false;
      }
      if (job.psold) delete job.psold;
      if (job.mb) delete job.mb;
    }
    else if (job.type_of_rating === 'MB') {
      if (!job.mb) {
        console.log("Need MB parameter!");
        return false;
      }
      else if (job.curve_id !== 58) {
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
    if (!submitingJob) return;
    if (!newJob || !newJob.parameter || !ValidateJob(newJob.parameter)) {
      setSubmitingJob(false);
      alert("Job parameter error!");
      return;
    }

    var form_data = new FormData();
    var js = newJob.parameter;
    js['job_guid'] = uuidv4();
    form_data.append('para', JSON.stringify(js));
    if (newJob.data_file) {
      if (js.data_source_type !== "User_Upload") js['data_correction'] = newJob.data_file.name;
      form_data.append("data", newJob.data_file);
    }

    fetch('/api/job', {
      method: "POST",
      body: form_data
    }).then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new TypeError("Oops, Submit job failed!");
    })
      .then(msg => {
        alert(msg);
      })
      .catch(error => {
        console.log(error);
      })
      .then(() => {
        setSubmitingJob(false);
      });
    // eslint-disable-next-line
  }, [submitingJob]);

  //submit jobrun
  React.useEffect(() => {
    if (!submitingJobRun) return;
    if (!newJob || !newJob.data_file || !newJob.parameter || !ValidateJob(newJob.parameter)) {
      setSubmitingJobRun(false);
      alert("Job parameter error!");
      return;
    }

    newJob.parameter['job_guid'] = uuidv4();
    var form_data = new FormData();
    var js = newJob.parameter;
    form_data.append("data", newJob.data_file);
    form_data.append('para', JSON.stringify(js));

    fetch('/api/job?jobrun=true', {
      method: "POST",
      body: form_data
    }).then(response => {
      if (response.ok) {
        return response.blob();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(blob => {
        var url = window.URL.createObjectURL(blob);
        var a = document.createElement('a');
        a.href = url;
        a.download = "pat_res.zip";
        document.body.appendChild(a); // we need to append the element to the dom -> otherwise it will not work in firefox
        a.click();
        a.remove();  //afterwards we remove the element again   
      })
      .catch(error => {
        console.log(error);
      })
      .then(() => {
        setSubmitingJobRun(false);
      });
    // eslint-disable-next-line
  }, [submitingJobRun]);

  const handleConfirm = (isOK) => {
    var it = confirm
    setConfirm('');
    if (isOK) {
      if (it === "submit the job") setSubmitingJob(true);
      else if (it === "submit the quick job") setSubmitingJobRun(true);
    }
  };

  const handleUpdateJob = (job) => {
    var para = job.parameter;
    if (para) {
      var sort = ["job_name",
        "data_source_type", "cat_data", "reference_job",
        "data_correction", "valid_rules",
        "type_of_rating", "curve_id", "psold", "FLS", "mb",
        "user_email", "user_name"
      ];

      var total = Object.keys(para);
      var s1 = sort.filter(k => total.includes(k));
      var s2 = total.filter(k => !s1.includes(k)).sort();

      var u = s1.pop()
      if (u === "user_name") {
        s2.push(u);
        u = s1.pop()
        if (u === "user_email") s2.push(u);
      }
      else s1.appen(u);

      var sorted = {}
      s1.concat(s2).forEach(k => {
        if (para[k]) sorted[k] = para[k];
      });
      para = sorted;
    }

    setNewJob({ ...job, parameter: para })
    setParaString(JSON.stringify(para, (k, v) => {
      if (k === 'blending_weights') return v.join();
      else return v;
    }, 4));
  };

  const peril_name = ["Earthquake", "Windstorm", "Severe Storm/Winterstorm", "Flood", "Wildfire", "Terrorism", "WorkersComp"]
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



  const handleStep = (step) => () => {
    setActiveStep(step);
  };

  const prepareDataChange = (job, ds_type = null, rt_type = null) => {
    if (ds_type) {
      if (ds_type === 'Reference_Job') {
        job['reference_job'] = { job_id: refJob.job_id }
        if (job.cat_data) {
          if (job.cat_data.server) setRefJob({ ...refJob, cat_data: job.cat_data })
          delete job.cat_data;
        }
        job.data_file = '';
      }
      else if (ds_type === 'User_Upload') {
        if (job.reference_job) delete job.reference_job;
        if (job.cat_data) {
          setRefJob({ ...refJob, cat_data: job.cat_data })
          delete job.cat_data;
        }
        job.data_file = '';
      }
      else if (ds_type === 'Cat_Data') {
        if (job.reference_job) delete job.reference_job;
        job.data_file = '';
        job['cat_data'] = refJob.cat_data ? refJob.cat_data :
          {
            server: "",
            edm: "",
            rdm: "",
            portinfoid: 0,
            perilid: 0,
            analysisid: 0
          };
      }
      job.data_source_type = ds_type;
    }

    if (rt_type) {
      if (job['type_of_rating'] === 'PSOLD' && job.psold) {
        setSavePsold({ ...job.psold });
        delete job.psold;
      }
      else if (job['type_of_rating'] === 'FLS' && job.fls) {
        setSaveFls({ ...job.fls });
        delete job.fls;
      }
      else if (job['type_of_rating'] === 'MB' && job.mb) {
        setSaveMb({ ...job.mb });
        delete job.mb
      }
      job.curve_id = 1;
      if (rt_type === "PSOLD") {
        if (newJob.parameter.coverage_type === 'Building_Only'
          || newJob.parameter.coverage_type === 'Contents_Only') job.curve_id = 2020;
        else job.curve_id = 2016;

        job['psold'] = { ...savePsold };
      }
      else if (rt_type === "MB") {
        job['mb'] =
        {
          "custom_type": saveMb ? saveMb.custom_type : 1
        };
      }

      job['type_of_rating'] = rt_type;
    }

    return job;
  };

  const handleNewRateType = (rt) => {
    var lst = [];
    if (rt === "PSOLD") lst = psold_curves;
    else if (rt === "FLS") lst = fls_curves
    else if (rt === "MB") lst = mb_curves

    setCurveList(lst);
  };

  const pulseLoading = () => {
    return loadingServerList || loadingPortList || loadingPerilList || loadingDbList || submitingJob || submitingJobRun;
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
              <Typography variant='h5' color="textSecondary" gutterBottom>
                New Analysys
              </Typography>
              <FormControl className={classes.formControl}>
                <Box
                  component="form"
                  sx={{
                    '& > :not(style)': { m: 1, width: '62ch' }
                  }}
                  noValidate
                  autoComplete="off"
                >
                  <TextField id="alae-basic" label="Job Name" variant="standard"
                    value={newJob.parameter.job_name}
                    onChange={event => {
                      if (newJob.parameter.job_name !== event.target.value) {
                        handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, job_name: event.target.value } });
                      }
                    }}
                  />
                </Box>
              </FormControl>
              <div class='row' style={{ marginLeft: '2px', marginTop: '20px' }}>
                <Button style={{ outline: 'none', height: '36px' }}
                  onClick={(e) => {
                    setConfirm("submit the job");
                  }}
                >Submit Job
                </Button>
                <Divider orientation="vertical" flexItem />
                <Button style={{ outline: 'none', height: '36px' }}
                  disabled={!newJob || !newJob.parameter || newJob.parameter.data_source_type !== 'User_Upload' || !newJob.data_file}
                  onClick={() => {
                    setConfirm("submit the quick job");
                  }}
                >Quick Job
                </Button>
              </div>
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
        <div box class="job_bottom_row">
          <Typography variant='h5' color="textSecondary" gutterBottom>
            Summary:
          </Typography>
          <div>
            <textarea value={paraString}
              readOnly={true}
              style={{
                fontSize: '14px',
                width: '100%', height: '50vh', padding: '10px', color: theme.palette.text.primary,
                background: theme.palette.background.default
              }}>
            </textarea>
          </div>
        </div>
      </div>
      <div class="job_right_col">
        <div>
          <div>
            <Stepper nonLinear activeStep={activeStep} style={{ padding: '10px', marginBottom: '5px' }} >
              {steps.map((label, index) => (
                <Step key={label} >
                  <StepButton color="textSecondary" onClick={(handleStep(index))}>
                    <div style={{ color: theme.palette.text.primary, background: theme.palette.background.default }}>
                      {label}
                    </div>
                  </StepButton>
                </Step>
              ))}
            </Stepper>
            <div id='step1' class='step_box' hidden={activeStep !== 0}>
              <Typography variant='h5' color="textSecondary" gutterBottom>
                Policy, Location, and Fac Data
              </Typography>
              <FormControl className={classes.formControl}>
                <InputLabel shrink id="rate-placeholder-label">
                  Data Source
                </InputLabel>
                <Select
                  labelId="ds-placeholder-label"
                  id="ds-placeholder"
                  value={newJob.parameter.data_source_type}
                  defaultValue={'Cat_Data'}
                  onChange={event => {
                    if (event.target.value !== newJob.parameter.data_source_type) {
                      var job = prepareDataChange(newJob.parameter, event.target.value);
                      handleUpdateJob({ ...newJob, parameter: { ...job } });
                      if (event.target.value === 'Cat_Data') {
                        setLoadingAnlsList(true);
                        setLoadingPortList(true);
                      }
                    }
                  }}
                >
                  {dataSrcList.map((n) => {
                    return <MenuItem value={n}>{ }{n}</MenuItem>
                  })}
                </Select>
              </FormControl>
              <div id='data_edm' hidden={newJob.parameter.data_source_type !== 'Cat_Data'} >
                <Autocomplete className={classes.formControl} style={{ width: '70%' }}
                  options={serverList}
                  inputValue={currServer}
                  noOptionsText="Enter to add a server"
                  onInputChange={(event, newValue) => {
                    if (!event && newValue === '') setCurrServer(newJob.parameter.cat_data?.server);
                    else setCurrServer(newValue);

                    if (event && (event.type === 'blur' || event.type === 'click' || (event.type === 'keydown' && event.key === 'Enter'))
                      && newValue !== newJob.parameter.cat_data.server) {
                      handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, cat_data: { ...newJob.parameter.cat_data, server: newValue, rdm: '', edm: '', portinfoid: 0, analysisid: 0 } } });
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
                <div>
                  <FormControl className={classes.formControl}>
                    <InputLabel shrink id="edm-placeholder-label">
                      <Tooltip title="Click to match RDM">
                        <Button style={{ padding: 0 }}
                          onClick={() => {
                            if (newJob.parameter.cat_data?.rdm) {
                              let rdm = newJob.parameter.cat_data.rdm.toLowerCase().replace('rdm', 'edm');
                              let s0 = 0;
                              let ee = ''
                              dbList.edm.forEach(edm => {
                                let s = similarity(rdm, edm.toLowerCase())
                                if (s > s0) {
                                  s0 = s;
                                  ee = edm
                                }
                              });

                              handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, cat_data: { ...newJob.parameter.cat_data, 'edm': ee } } });
                              setLoadingPortList(true);
                            }
                          }}
                        >
                          EDM
                        </Button>
                      </Tooltip>
                    </InputLabel>
                    <Select
                      labelId="edm-placeholder-label"
                      id="edm-placeholder"
                      value={newJob.parameter.cat_data?.edm}
                      defaultValue={''}
                      onChange={event => {
                        if (newJob.parameter.cat_data.edm !== event.target.value) {
                          handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, cat_data: { ...newJob.parameter.cat_data, edm: event.target.value, portinfoid: 0 } } });
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
                      value={newJob.parameter.cat_data?.portinfoid}
                      defaultValue={''}
                      onChange={event => {
                        if (newJob.parameter.cat_data.portinfoid !== event.target.value) {
                          handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, cat_data: { ...newJob.parameter.cat_data, portinfoid: event.target.value } } });
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
                      value={newJob.parameter.cat_data?.perilid}
                      defaultValue={''}
                      onChange={event => {
                        if (newJob.parameter.cat_data.perilid !== event.target.value) {
                          handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, cat_data: { ...newJob.parameter.cat_data, perilid: event.target.value } } });
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
                      <Tooltip title="Click to match EDM">

                        <Button style={{ padding: 0 }}
                          onClick={() => {
                            let edm = newJob.parameter.cat_data?.edm.toLowerCase().replace('edm', 'rdm');
                            let s0 = 0;
                            let r = ''
                            dbList.rdm.forEach(rdm => {

                              let s = similarity(edm, rdm.toLowerCase())
                              if (s > s0) {
                                s0 = s;
                                r = rdm
                              }
                            });

                            handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, cat_data: { ...newJob.parameter.cat_data, 'rdm': r } } });
                            setLoadingAnlsList(true);
                          }}
                        >
                          RDM
                        </Button>
                      </Tooltip>
                    </InputLabel>
                    <Select
                      labelId="rdm-placeholder-label"
                      id="rdm-placeholder"
                      value={newJob.parameter.cat_data?.rdm}
                      defaultValue={''}
                      onChange={event => {
                        if (newJob.parameter.cat_data.rdm !== event.target.value) {
                          handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, cat_data: { ...newJob.parameter.cat_data, rdm: event.target.value, analysisid: 0 } } });
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
                      value={newJob.parameter.cat_data?.analysisid}
                      defaultValue={''}
                      onChange={event => {
                        if (newJob.parameter.cat_data.analysisid !== event.target.value) {
                          handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, cat_data: { ...newJob.parameter.cat_data, analysisid: event.target.value } } });
                        }
                      }}
                    >
                      {anlsList?.map((a) => {
                        return <MenuItem value={a.id}>{ }{a.id > 0 ? '(' + a.id + ') ' + a.name : ''}</MenuItem>
                      })}
                    </Select>
                  </FormControl>
                </div>
              </div>
              <div id='data_ref' hidden={newJob.parameter.data_source_type !== 'Reference_Job'} >
                <Typography className={classes.c_label} color="textPrimary" gutterBottom>
                  <ul><li>Use layer data from reference job: {refJob.job_id}</li></ul>
                </Typography>
              </div>
              <div>
                <FormControl className={classes.formControl}>
                  <Box
                    component="form"
                    sx={{
                      '& > :not(style)': { m: 1, width: '60ch' }
                    }}
                    noValidate
                    autoComplete="off"
                  >
                    <TextField id="df-basic" label={"Data " + (newJob.parameter.data_source_type === 'User_Upload' ? '' : 'Correction') + " File"} variant="standard" readOnly
                      value={(newJob.data_file ? newJob.data_file.name : "No Data")}
                    />
                  </Box>
                  <div class="row" style={{ alignItems: 'center' }} >
                    <div class="col-md-4 align-left vertical-align-top">
                      <Button variant="raised" component="label" className={classes.button}
                      > Select File
                        <input hidden
                          className={classes.input}
                          style={{ display: 'none' }}
                          id="raised-button-file"
                          type="file"
                          onChange={(e) => {
                            if (e.target.files && e.target.files.length > 0) {
                              var job = newJob.parameter;
                              if (job.data_source_type !== 'User_Upload') {
                                job["data_correction"] = e.target.files[0].name;
                              }
                              handleUpdateJob({ ...newJob, data_file: e.target.files[0] })
                            }
                          }}
                        />
                      </Button>
                    </div>
                    <div class="col-md-4 align-left vertical-align-top">
                      <Button variant="raised" component="span" className={classes.button}
                        onClick={(e) => { handleUpdateJob({ ...newJob, data_file: null }) }}>
                        Remove
                      </Button>
                    </div>
                  </div>
                </FormControl>
              </div>
              <Typography className={classes.c_label} variant='h6' color="textSecondary" gutterBottom>
                Data Validation Options
              </Typography>
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
                              handleUpdateJob({
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
            </div>
            <div id='step2' class='step_box' hidden={activeStep !== 1}>
              <Typography variant='h5' color="textSecondary" gutterBottom>
                Type of Rating
              </Typography>
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
                        job = prepareDataChange(job, null, event.target.value);
                        handleUpdateJob({ ...newJob, parameter: { ...job } });
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

                        if (job['type_of_rating'] === 'PSOLD') {
                          if (event.target.value === 2016) {
                            job.psold.trend_factor = 1.035
                            if (job.coverage_type === 'Building_Only' || job.coverage_type === 'Contents_Only')
                              job.coverage_type = 'Building_Contents';
                          }
                          else if (event.target.value === 2020) {
                            job.psold.trend_factor = 1.05;
                          }
                        }

                        job['curve_id'] = event.target.value;

                        if (job['type_of_rating'] === 'FLS' && event.target.value === 57) job['fls'] = { ...saveFls };
                        else if (job['type_of_rating'] === 'MB' && event.target.value === 58)
                          job['mb'] = { ...job.mb, b: saveMb ? saveMb.b : 0, g: saveMb ? saveMb.g : 0 };
                        handleUpdateJob({ ...newJob, parameter: { ...job } });
                      }
                    }}
                  >
                    {curveList.map((n) => {
                      return <MenuItem value={n.ID}>{ }{newJob.parameter.type_of_rating === 'PSOLD' ? n.name : n.ID + ' -- ' + n.name}</MenuItem>
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
                          handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, psold: { ...newJob.parameter.psold, curve_persp: event.target.value } } });
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
                          handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, psold: { ...newJob.parameter.psold, peril_subline: event.target.value } } });
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
                            handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, psold: { ...newJob.parameter.psold, trend_factor: parseFloat(event.target.value) } } });
                          }
                        }}
                      />
                    </Box>
                  </FormControl>
                  <FormControl className={classes.formControl} style={{ color: theme.palette.text.primary, background: theme.palette.background.default}}>
                    <LocalizationProvider style={{ color: theme.palette.text.primary, background: theme.palette.background.default}} dateAdapter={AdapterDateFns}>
                      <DatePicker style={{ color: theme.palette.text.primary, background: theme.palette.background.default}}
                        label="Average Accident Date"
                        value={newJob.parameter.psold?.average_accident_date}
                        format="MM/DD/YYYY"
                        onChange={(newValue) => {
                          handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, psold:{...newJob.parameter.psold, average_accident_date: newValue.toLocaleDateString('en-US') }}});
                        }}
                        renderInput={(params) => <TextField {...params} />}
                      />
                    </LocalizationProvider>
                  </FormControl>
                </div>
                <Typography variant='h6' className={classes.c_label} color="textSecondary" gutterBottom>
                  PSOLD Blending
                </Typography>
                <div>
                <FormControl component="fieldset">
              </FormControl>
              <FormControl className={classes.formControl} style={{width: '35vh' }} >
                  <InputLabel shrink id="coverage-placeholder-label">
                    Coverage Type
                  </InputLabel>
                  <Select
                    labelId="blend-t-placeholder-label"
                    id="blend-t-placeholder"
                    value={newJob.parameter.psold?.blending_type}
                    defaultValue={'no_blending'}
                    onChange={event => {
                      if (newJob.parameter.psold && newJob.parameter.psold.blending_type !== event.target.value) {
                        handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, psold: { ...newJob.parameter.psold, blending_type: event.target.value } } });
                      }
                    }}
                  >
                    {blending_types.map((n) => {
                      return <MenuItem value={n.id}>{ }{n.name}</MenuItem>
                    })}
                  </Select>
                </FormControl>
                <FormControl className={classes.formControl} style={{ marginLeft: '30px', marginTop: '20px'}} 
                  hidden={!newJob.parameter.psold || newJob.parameter.psold.blending_type === 'no_blending'} >
                    <FormControlLabel control={
                      <Checkbox  style={{ color: theme.palette.text.primary, background: theme.palette.background.default}}
                        checked={newJob && newJob.parameter && newJob.parameter.psold?.hpr_blending}
                        onChange={event => {
                          handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, psold: { ...newJob.parameter.psold, hpr_blending: event.target.checked } } });
                        }}
                      />} label={"Use HPR blending"}
                    />
                  </FormControl>
                  <FormControl className={classes.formControl} hidden={!newJob.parameter.psold || newJob.parameter.psold.blending_type === 'no_blending'}>
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
                                  job['psold']['blending_weights'] = psoldBlending.map(w => w.weight)
                                  handleUpdateJob({ ...newJob, parameter: { ...job } });
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
                </div>
              </div>

              <div id='fls_para' hidden={newJob.parameter.type_of_rating !== 'FLS' || !newJob.parameter.fls
                || newJob.parameter.curve_id !== 57}>
                <FormControl className={classes.formControl}>
                  <Box component="form" sx={{ '& > :not(style)': { m: 1, width: '18ch' }, }}
                    noValidate
                    autoComplete="off"
                  >
                    <TextField label="mu" variant="standard" type='number'
                      value={newJob.parameter.fls?.mu}
                      inputProps={{ maxLength: 13, step: "0.01" }}
                      onChange={event => {
                        if (newJob.parameter.fls?.mu !== event.target.value) {
                          handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, fls: { ...newJob.parameter.fls, mu: parseFloat(event.target.value) } } });
                        }
                      }}
                    />
                  </Box>
                </FormControl>
                <FormControl className={classes.formControl}>
                  <Box component="form" sx={{ '& > :not(style)': { m: 1, width: '18ch' }, }}
                    noValidate
                    autoComplete="off"
                  >
                    <TextField label="w" variant="standard" type='number'
                      value={newJob.parameter.fls?.w}
                      inputProps={{ maxLength: 13, step: "0.1" }}
                      onChange={event => {
                        if (newJob.parameter.fls?.w !== event.target.value) {
                          handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, fls: { ...newJob.parameter.fls, w: parseFloat(event.target.value) } } });
                        }
                      }}
                    />
                  </Box>
                </FormControl>
                <FormControl className={classes.formControl}>
                  <Box component="form" sx={{ '& > :not(style)': { m: 1, width: '18ch' }, }}
                    noValidate
                    autoComplete="off"
                  >
                    <TextField label="tau" variant="standard" type='number'
                      value={newJob.parameter.fls?.tau}
                      inputProps={{ maxLength: 13, step: "0.1" }}
                      onChange={event => {
                        if (newJob.parameter.fls?.tau !== event.target.value) {
                          handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, fls: { ...newJob.parameter.fls, tau: parseFloat(event.target.value) } } });
                        }
                      }}
                    />
                  </Box>
                </FormControl>
                <FormControl className={classes.formControl}>
                  <Box component="form" sx={{ '& > :not(style)': { m: 1, width: '18ch' }, }}
                    noValidate
                    autoComplete="off"
                  >
                    <TextField label="theta" variant="standard" type='number'
                      value={newJob.parameter.fls?.theta}
                      inputProps={{ maxLength: 13, step: "0.01" }}
                      onChange={event => {
                        if (newJob.parameter.fls?.theta !== event.target.value) {
                          handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, fls: { ...newJob.parameter.fls, theta: parseFloat(event.target.value) } } });
                        }
                      }}
                    />
                  </Box>
                </FormControl>
                <FormControl className={classes.formControl}>
                  <Box component="form" sx={{ '& > :not(style)': { m: 1, width: '18ch' }, }}
                    noValidate
                    autoComplete="off"
                  >
                    <TextField label="beta" variant="standard" type='number'
                      value={newJob.parameter.fls?.beta}
                      inputProps={{ maxLength: 13, step: "0.01" }}
                      onChange={event => {
                        if (newJob.parameter.fls?.beta !== event.target.value) {
                          handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, fls: { ...newJob.parameter.fls, beta: parseFloat(event.target.value) } } });
                        }
                      }}
                    />
                  </Box>
                </FormControl>
                <FormControl className={classes.formControl}>
                  <Box component="form" sx={{ '& > :not(style)': { m: 1, width: '18ch' }, }}
                    noValidate
                    autoComplete="off"
                  >
                    <TextField label="cap" variant="standard" type='number'
                      value={newJob.parameter.fls?.cap}
                      inputProps={{ maxLength: 13, step: "0.1" }}
                      onChange={event => {
                        if (newJob.parameter.fls?.cap !== event.target.value) {
                          handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, fls: { ...newJob.parameter.fls, cap: parseFloat(event.target.value) } } });
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
                          handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, mb: { ...newJob.parameter.mb, custom_type: parseInt(event.target.value) } } });
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
                    <Box component="form" sx={{ '& > :not(style)': { m: 1, width: '20ch' }, }}
                      noValidate
                      autoComplete="off"
                    >
                      <TextField label="b" variant="standard" type='number'
                        value={newJob.parameter.mb?.b}
                        inputProps={{ maxLength: 13, step: "0.01" }}
                        onChange={event => {
                          if (newJob.parameter.mb?.b !== event.target.value) {
                            handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, mb: { ...newJob.parameter.mb, b: parseFloat(event.target.value) } } });
                          }
                        }}
                      />
                    </Box>
                  </FormControl>
                  <FormControl className={classes.formControl}>
                    <Box component="form" sx={{ '& > :not(style)': { m: 1, width: '20ch' }, }}
                      noValidate
                      autoComplete="off"
                    >
                      <TextField label="g" variant="standard" type='number'
                        value={newJob.parameter.mb?.g}
                        inputProps={{ maxLength: 13, step: "1" }}
                        onChange={event => {
                          if (newJob.parameter.mb?.g !== event.target.value) {
                            handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, mb: { ...newJob.parameter.mb, g: parseFloat(event.target.value) } } });
                          }
                        }}
                      />
                    </Box>
                  </FormControl>
                </div>
              </div>
            </div>
            <div id='step3' class='step_box' hidden={activeStep !== 2}>
              <Typography variant='h5' color="textSecondary" gutterBottom>
                Other Settings
              </Typography>
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
                          handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, loss_alae_ratio: parseFloat(event.target.value) } });
                        }
                      }}
                    />
                  </Box>
                </FormControl>
                <FormControl className={classes.formControl}>
                  <InputLabel shrink id="coverage-placeholder-label">
                    Coverage Type
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
                          && para.type_of_rating === "PSOLD") {
                          if (para.curve_id === 2016) {
                            para.curve_id = 2020;
                            if (para.trend_factor === 1.035) para.tr = 1.05;
                          }
                        }

                        handleUpdateJob({ ...newJob, parameter: { ...para } });
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
                          handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, additional_coverage: parseFloat(event.target.value) } });
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
                        handleUpdateJob({ ...newJob, parameter: { ...newJob.parameter, deductible_treatment: event.target.value } });
                      }
                    }}
                  >
                    {['Retains_Limit', 'Erodes_Limit']
                      .map((n) => {
                        return <MenuItem value={n}>{ }{n}</MenuItem>
                      })}
                  </Select>
                </FormControl>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};