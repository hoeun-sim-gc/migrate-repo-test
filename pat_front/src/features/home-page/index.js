import React, { useState, useContext, useCallback, useRef } from 'react';
import {useHistory} from 'react-router-dom';

import Tooltip from '@mui/material/Tooltip';

import { makeStyles, useTheme } from '@material-ui/core/styles';
import {Divider, Grid, Button} from '@material-ui/core';

import { PulseLoader } from "react-spinners";
import BootstrapTable from 'react-bootstrap-table-next';
import paginationFactory from 'react-bootstrap-table2-paginator';
import ToolkitProvider, { Search } from 'react-bootstrap-table2-toolkit';
import 'bootstrap/dist/css/bootstrap.min.css';
import 'react-bootstrap-table-next/dist/react-bootstrap-table2.min.css';

import Dialog from '@mui/material/Dialog';
import DialogActions from '@mui/material/DialogActions';
import DialogContent from '@mui/material/DialogContent';
import DialogContentText from '@mui/material/DialogContentText';
import DialogTitle from '@mui/material/DialogTitle';

import WbMenu from '../../app/menu';

import { UserContext } from "../../app/user-context";
import columns from './header';
import { convertTime } from '../../app/theme'

import './index.css';
import { Checkbox } from '@mui/material';
import FormControlLabel from '@mui/material/FormControlLabel';

const useStyles = makeStyles((theme) => ({
  root: {
    flexGrow: 1,
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
  para: {
    color: theme.palette.text.primary,
    marginTop: 10,
    marginBottom: 10,
    overflow: "auto",
    fontSize: 16,
    padding: 5
  },
}));

export default function HomePage(props) {
  const history = useHistory(); 
  const classes = useStyles();
  const theme = useTheme();

  const tableRef = useRef(null);

  const [user,] = useContext(UserContext);

  const [multiSel, setMultiSel]=useState(false);
  const [confirm, setConfirm] = React.useState('');

  const [loadingJobList, setLoadingJobList] = useState(false);
  const [currentJob, setCurrentJob] = useState(null);
  const [jobList, setJobList] = useState([]);

  const [loadingJobPara, setLoadingJobPara] = useState(false);
  const [currentPara, setCurrentPara] = useState(null);

  const [loadingJobSum, setLoadingJobSum] = useState(false);
  const [currentSum, setCurrentSum] = useState([]);

  const [downloadingResults, setDownloadingResults] = useState(false);
  const [downloadingData, setDownloadingData] = useState(false);
  const [flaggedOnly, setFlaggedOnly] = useState(true);

  React.useEffect(() => {
    setLoadingJobList(true);
    setMultiSel(localStorage.getItem('job_multi_sel')==='true');

    const interval = setInterval(() => setLoadingJobList(true), 60000);
    return () => {
      clearInterval(interval);
    };
  }, []);

  //job list
  React.useEffect(() => {
    if (!loadingJobList) return;
    
    const request = '/api/job' + (user.email!="admin.pat@guycarp.com"? '?user='+user.email.toLowerCase():'');
    fetch(request).then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(data => {
        //data = data.filter(u => u.user_email.toLowerCase() === user.email.toLowerCase())
        data.forEach(job => {
          job.receive_time = convertTime(job.receive_time);
          job.update_time = convertTime(job.update_time);
        });
        data.sort((a, b) => (a.update_time > b.update_time) ? -1 : 1);
        setJobList(data);

        if(currentJob && currentJob.job_id>0)
        {
          var sel = data.find(j=>j.job_id===currentJob.job_id);
          if(sel)  setCurrentJob(sel);
        }
        if(data.length>0 &&(!currentJob || currentJob.job_id <=0 ))
        {
          setCurrentJob(data[0]);  
          tableRef.current.selectionContext.selected.push(data[0].job_id);
        }
      })
      .catch(error => {
        console.log(error);
      })
      .then(() => {
        setLoadingJobList(false);
      });
    // eslint-disable-next-line
  }, [loadingJobList]);

  React.useEffect(() => {
    setCurrentPara('');
    setLoadingJobPara(true);

    setCurrentSum([]);
    setLoadingJobSum(true);
  }, [currentJob]);

  //job para
  React.useEffect(() => {
    if (!loadingJobPara) return;
    if (!currentJob) {
      setLoadingJobPara(false);
      return;
    }

    const request = '/api/para/' + currentJob.job_id
    fetch(request).then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(data => {
        setCurrentPara(JSON.stringify(data, null,'  '));
      })
      .catch(error => {
        console.log(error);
      })
      .then(() => {
        setLoadingJobPara(false);
      });
    // eslint-disable-next-line
  }, [loadingJobPara, currentJob]);

  //job summary
  React.useEffect(() => {
    if (!loadingJobSum) return;
    if (!currentJob) {
      setLoadingJobSum(false);
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
        setCurrentSum(data);
      })
      .catch(error => {
        console.log(error);
      })
      .then(() => {
        setLoadingJobSum(false);
      });
    // eslint-disable-next-line
  }, [loadingJobSum, currentJob]);

  //results
  React.useEffect(() => {
    if (!downloadingResults) return;
    
    var lst= tableRef.current.selectionContext.selected;
    if(lst.length<=0) {
      setDownloadingResults(false);
      alert("No analysis is selected!");
      return;
    }

    let request = '/api/result/' + lst.join('_');
    fetch(request).then(response => {
      if (response.ok) {
        return response.blob();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(blob => {
        var url = window.URL.createObjectURL(blob);
        var a = document.createElement('a');
        a.href = url;
        a.download = "pat_result_" + currentJob.job_id + ".zip";
        document.body.appendChild(a); // we need to append the element to the dom -> otherwise it will not work in firefox
        a.click();
        a.remove();  //afterwards we remove the element again   
      })
      .catch(error => {
        console.log(error);
      })
      .then(() => {
        setDownloadingResults(false);
      });
    // eslint-disable-next-line
  }, [downloadingResults]);

  const handleNewJob = useCallback((job_id) => history.push('/job' + (job_id?'/'+job_id:'') ), [history]);
  
  const handleStopJob = ()=>{
    var lst= tableRef.current.selectionContext.selected;
    if(lst.length<=0) {
      setDownloadingResults(false);
      alert("No analysis is selected!");
      return;
    }

    let request = '/api/stop/' + lst.join('_');
    fetch(request,{method: "POST"}).then(response => {
      if (response.ok) {
        setLoadingJobList(true);
      }
    });    
  };
  
  const handleRunJob = (id)=>{
    let request = '/api/run/' + id;
    fetch(request, {method: "POST"}).then(response => {
      if (response.ok) {
        setLoadingJobList(true);
      }
    }); 
  };

  //data file
  React.useEffect(() => {
    if (!downloadingData) return;
    if (!currentJob) {
      setDownloadingData(false);
      alert("No analysis is selected!");
      return;
    }

    let request = '/api/valid/' + currentJob.job_id + '?flagged='+ flaggedOnly;
    fetch(request).then(response => {
      if (response.ok) {
        return response.blob();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(blob => {
        var url = window.URL.createObjectURL(blob);
        var a = document.createElement('a');
        a.href = url;
        a.download = "pat_validation" + currentJob.job_id + ".zip";
        document.body.appendChild(a); // we need to append the element to the dom -> otherwise it will not work in firefox
        a.click();
        a.remove();  //afterwards we remove the element again   
      })
      .catch(error => {
        console.log(error);
      })
      .then(() => {
        setDownloadingData(false);
      });
    // eslint-disable-next-line
  }, [downloadingData]);

  const get_options = ()=>{
    let ps = 20;
    try
    {
      var s = localStorage.getItem('job_page_size');
      ps = parseInt(s);
    }
    catch
    {
      ps=20;
    }

    if (!ps || ps <= 0) ps=20;

    return {
      // pageStartIndex: 0,
      sizePerPage: ps, 
      hideSizePerPage: true,
      hidePageListOnlyOnePage: true,
      sizePerPageList: [5,10,15,20,25,30,40,50],
      alwaysShowAllBtns: true,
      showTotal: true
    };
  };
  
  const handleSelChange = (event) => {
    setMultiSel(event.target.checked);
    localStorage.setItem('job_multi_sel', event.target.checked);
    window.location.reload(false);
  };

  const handleConfirm = (isOK) => {
    var it = confirm
    setConfirm('');
    if (isOK) {
      if(it === "stop the selected jobs" ) handleStopJob()
      else if(it === "start (rerun) the selected job"  && currentJob) handleRunJob(currentJob.job_id);
    }
  };

  const get_select_row = ()=>{
    var sel = multiSel?'checkbox':'radio';
    return  {
      mode: sel,
      clickToSelect: true,
      style: { backgroundColor: theme.palette.action.selected, fontWeight: 'bold' },
      onSelect: (row, isSelect) => {
        if (isSelect) {
          setCurrentJob(row);
        }
      }
    };
  };

  const { SearchBar } = Search;

  return (
    <div class="pat_container">
      <div class="job_col">
        {(loadingJobList || loadingJobPara || downloadingResults || downloadingData) &&
          <div className={classes.spinner}>
            <PulseLoader
              size={30}
              color={"#2BAD60"}
              loading={loadingJobList || loadingJobPara || downloadingResults || downloadingData}
            />
          </div>
        }
        
        <div class='row' style={{marginLeft: '2px'}}>
          <Tooltip title="Refresh job list"  >
            <Button style={{outline: 'none', height:'36px'}}
                onClick={(e) => { setLoadingJobList(true); }}
              >Refresh
            </Button>
          </Tooltip>
          <Divider orientation="vertical" flexItem />
          <Tooltip title="Stop selected analyses"  >
            <Button style={{outline: 'none', height:'36px'}}
                onClick={(e) => { setConfirm("stop the selected jobs"); }}
              >Stop
            </Button>
          </Tooltip>
          <Tooltip title="Run selected analysis"  >
            <Button style={{outline: 'none', height:'36px'}}
                onClick={(e) => { setConfirm("start (rerun) the selected job"); }}
              >Start
            </Button>
          </Tooltip>
          <Divider orientation="vertical" flexItem />
          <Tooltip title="Download Results">
            <Button style={{outline: 'none', height:'36px'}}
                onClick={(e) => { setDownloadingResults(true); }}
              >Results
            </Button>
          </Tooltip>
          <Tooltip title="Export detail data for validation">
             <WbMenu header="Export" items={[
                { text: 'All Data', onClick: () => { setFlaggedOnly(false);  setDownloadingData(true) } },
                { text: 'Flagged Data', onClick: () => { setFlaggedOnly(true);  setDownloadingData(true)} },
              ]} />
          </Tooltip>
          {/* <Divider orientation="vertical" flexItem />
          <Tooltip title="Populate allocated premium back to EDM">
            <Button style={{outline: 'none', height:'36px'}}
                onClick={(e) => { alert("This option hasn't been implemented yet!"); }}
              >EDM
            </Button>
          </Tooltip> */}
          <Divider orientation="vertical" flexItem />
          <Tooltip title="Copy settings to a new analysis"  >
            <Button style={{outline: 'none', height:'36px'}}
                onClick={(e) => { handleNewJob(currentJob?.job_id); }}
              >Copy
            </Button>
          </Tooltip>
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
        <Grid container className={classes.root} spacing={2}>
          <Grid item md={12} style={{ marginTop: '-40px'}}>
            <ToolkitProvider
              keyField="job_id"
              data={jobList}
              columns={columns}
              bootstrap4
              search
            >
              {
                props => (
                  <div justify='flex-end'>
                    <Grid container justify='flex-end'>
                      <Grid item md={6} container justify='flex-end' alignItems='center' >
                        <FormControlLabel control={<Checkbox checked={multiSel} onChange={handleSelChange} />} label="Multi-Selection" />
                        <SearchBar  {...props.searchProps} style={{ height: '26px', width:'180px' }} />
                      </Grid>
                    </Grid>
                    <BootstrapTable classes={classes.table}
                      ref={tableRef} 
                      {...props.baseProps}
                      rowClasses={classes.table_row}
                      selectRow={get_select_row()}
                      pagination={paginationFactory(get_options())}
                      striped
                      hover
                      condensed
                    />
                  </div>
                )
              }
            </ToolkitProvider>
          </Grid>

        </Grid>
      </div>
      <div class="para_col para_container1" style={{ marginTop: '5px'}} >
        <div class="single_row">
          <h5>Parameters:</h5>
          <span>{currentJob?.job_id}</span>
        </div>
      <textarea value={currentPara}
          readOnly={true}
          class="para_row"
          style={{ color: theme.palette.text.primary, background: theme.palette.background.default }}>
        </textarea>
        <div class="sum_row">
          <h5>Summary:</h5>
          <table style={{ width: '100%', border: '1px solid gray' }} >
            <thead>
              <tr>
                <th>Item</th>
                <th>Count</th>
              </tr>
            </thead>
            <tbody>
              {currentSum.map(row => (
                <tr key={row.item}>
                  <td>{row.item}</td>
                  <td>{row.cnt}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};
