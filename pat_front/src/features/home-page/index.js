import React, { useState, useContext } from 'react';

import Tooltip from '@mui/material/Tooltip';

import { makeStyles, useTheme } from '@material-ui/core/styles';
import { Divider, Grid, Button,
  FormControl,FormControlLabel, FormLabel,RadioGroup, Radio } from '@material-ui/core';

import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faStepBackward, faFastBackward,faStop, faPlay,faSyncAlt, faDownload } from '@fortawesome/free-solid-svg-icons';

import { PulseLoader } from "react-spinners";
import BootstrapTable from 'react-bootstrap-table-next';
import paginationFactory from 'react-bootstrap-table2-paginator';
import ToolkitProvider, { Search } from 'react-bootstrap-table2-toolkit';
import 'bootstrap/dist/css/bootstrap.min.css';
import 'react-bootstrap-table-next/dist/react-bootstrap-table2.min.css';
import cellEditFactory from 'react-bootstrap-table2-editor';

import Dialog from '@mui/material/Dialog';
import DialogActions from '@mui/material/DialogActions';
import DialogContent from '@mui/material/DialogContent';
import DialogContentText from '@mui/material/DialogContentText';
import DialogTitle from '@mui/material/DialogTitle';

import WbMenu from '../../app/menu';

import { Allotment } from "allotment";
import "allotment/dist/style.css";

import { UserContext } from "../../app/user-context";
import columns from './header';
import { convertTime, calcDuration } from '../../app/theme'

import './index.css';

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
  const classes = useStyles();
  const theme = useTheme();

  const [user, setUser] = useContext(UserContext);

  const [confirm, setConfirm] = React.useState('');
  
  const [loadingJobList, setLoadingJobList] = useState(false);
  const [jobList, setJobList] = useState([]);

  const [currentJob, setCurrentJob] = useState(null);
  const [loadingJobPara, setLoadingJobPara] = useState(false);
  const [currentPara, setCurrentPara] = useState({job_id:'', parameter: '', summary: [] });
  const [updatingCurrent, setUpdatingCurrent] = useState(false);

  const [downloadingData, setDownloadingData] = useState(false);
  const [downloadOption, setDownloadOption] = React.useState({
    'isOpen': false,
    'data_type': 'results'
  });

  React.useEffect(() => {
    setLoadingJobList(true);

    const interval = setInterval(() => setUpdatingCurrent(true), 10000);
    return () => {
      clearInterval(interval);
    };
  }, []);

  //job list
  React.useEffect(() => {
    if (!loadingJobList) return;

    const request = '/api/job?req_id=' + btoa(user?.email?.toLowerCase());
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
          job.start_time = convertTime(job.start_time);
          job.finish_time = convertTime(job.finish_time);
          job.duration = calcDuration(job.start_time, job.finish_time)
        });
        data.sort((a, b) => {
          var diff = (Date.parse(b.finish_time) - Date.parse(a.finish_time));
          if (!isNaN(diff)) {
            if (diff > 0) return 1;
            else if (diff < 0) return -1;
          }
          diff = (Date.parse(b.start_time) - Date.parse(a.start_time));
          if (!isNaN(diff)) {
            if (diff > 0) return 1;
            else if (diff < 0) return -1;
          }
          diff = (Date.parse(b.receive_time) - Date.parse(a.receive_time));
          if (!isNaN(diff)) {
            if (diff > 0) return 1;
            else if (diff < 0) return -1;
          }

          return 0;
        });
        setJobList(data);

        if (currentJob && currentJob.job_id > 0) {
          var sel = data.find(j => j.job_id === currentJob.job_id);
          if (sel) {
            setCurrentJob(sel);
            setUser({ ...user, curr_job: sel.job_id });
          }
        }
        if (data.length > 0 && (!currentJob || currentJob.job_id <= 0)) {
          setCurrentJob(data[0]);
          setUser({ ...user, curr_job: data[0].job_id });
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

  //Update runing job status
  React.useEffect(() => {
    if (!updatingCurrent) return;
    if (!currentJob || currentJob.status === 'finished' || currentJob.status === 'error') {
      setUpdatingCurrent(false);
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
        var lst = JSON.parse(JSON.stringify(jobList));
        if (lst) {
          var sel = lst.find(j => j.job_id === currentJob.job_id);
          if (sel) {
            sel.status = data.status;
            sel.start_time = convertTime(data.start_time);
            if (data.status === 'finished') {
              sel.finish_time = convertTime(data.finish_time);
              sel.duration = calcDuration(data.start_time, data.finish_time)
            }
            setJobList(lst);
          }
        }
        setCurrentPara({
          job_id: data['job_id'],
          parameter: JSON.stringify(JSON.parse(data['parameters']), null, '    '),
          summary: data['summary']
        });
      })
      .catch(error => {
        console.log(error);
      })
      .then(() => {
        setUpdatingCurrent(false);
      });
    // eslint-disable-next-line
  }, [updatingCurrent]);

  //job para and summary
  React.useEffect(() => {
    if (!currentJob) return;

    setLoadingJobPara(true);
    const request = '/api/job/' + currentJob.job_id;
    fetch(request).then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(data => {
        if (data['job_id'] === currentJob.job_id){
          setCurrentPara({
            job_id : data['job_id'],
            parameter: JSON.stringify(JSON.parse(data['parameters']), null, '    '),
            summary: data['summary']
          });
        }        
      })
      .catch(error => {
        console.log(error);
      })
      .then(()=>{
        setLoadingJobPara(false);
      });

    // eslint-disable-next-line
  }, [currentJob]);

  const handleStopJob = () => {
    if (!currentJob) {
      setDownloadingData(false);
      alert("No analysis is selected!");
      return;
    }

    let request = '/api/stop/' + currentJob.job_id;
    fetch(request, { method: "POST" }).then(response => {
      if (response.ok) {
        setLoadingJobList(true);
      }
    });
  };
  
  const handleResetJob = (keep) => {
    if (!currentJob) {
      setDownloadingData(false);
      alert("No analysis is selected!");
      return;
    }

    let request = '/api/reset/' + currentJob.job_id + "?keep_data=" + (keep?'true':'false');
    fetch(request, { method: "POST" }).then(response => {
      if (response.ok) {
        setLoadingJobList(true);
      }
    });
  };

  const handleRenameJob = (job_id, name) => {
    let request = '/api/rename/' + job_id + "/" + name;
    fetch(request, { method: "PUT" }).then(response => {
      if (response.ok) {
        setLoadingJobList(true);
      }
    });
  };

  const handleRunJob = (id) => {
    let request = '/api/run/' + id;
    fetch(request, { method: "POST" }).then(response => {
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
  
      let request = '/api/data/' + currentJob.job_id +'?data_type='+ downloadOption.data_type;
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
          a.download = "pat_data_"+currentJob.job_id+".zip";
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

  const get_options = () => {
    let ps = 20;
    try {
      var s = localStorage.getItem('job_page_size');
      ps = parseInt(s);
    }
    catch
    {
      ps = 20;
    }

    if (!ps || ps <= 0) ps = 20;

    return {
      // pageStartIndex: 0,
      sizePerPage: ps,
      hideSizePerPage: false,
      hidePageListOnlyOnePage: true,
      sizePerPageList: [5, 10, 15, 20, 25, 30, 40, 50],
      alwaysShowAllBtns: true,
      showTotal: true,
      onSizePerPageChange:(s, p)=>
      {
        localStorage.setItem('job_page_size', s);
      } 
    };
  };

  const handleConfirm = (isOK) => {
    var it = confirm
    setConfirm('');
    if (isOK) {
      if (it === "reset the selected job but keep the input data") handleResetJob(true)
      else if (it === "reset the selected job to initial submit state") handleResetJob(false)
      else if (it === "stop/cancel the selected job") handleStopJob()
      else if (it === "run the selected job" && currentJob) handleRunJob(currentJob.job_id);
    }
  };

  const handleDownloadOpt = (isOK) => {
    setDownloadOption({...downloadOption, isOpen:false});
    if (isOK && currentJob){
      setDownloadingData(true);
    }
  };


  const select_row = {
      mode: 'radio',
      clickToSelect: true,
      clickToEdit: true,
      style: { backgroundColor: theme.palette.action.selected, fontWeight: 'bold' },
      onSelect: (row, isSelect) => {
        if (isSelect) {
          setCurrentJob(row);
          setUser({ ...user, curr_job: row.job_id })
        }
      }
  };

  const { SearchBar } = Search;

  return (
    <div class="pat_container">
      <Allotment defaultSizes={[70, 30]}>
        <div class="pane_cont">
          <div class="job_col">
            {(loadingJobList || loadingJobPara || downloadingData) &&
              <div className={classes.spinner}>
                <PulseLoader
                  size={30}
                  color={"#2BAD60"}
                  loading={loadingJobList || loadingJobPara || downloadingData}
                />
              </div>
            }

            <div class='row' style={{ marginLeft: '2px' }}>
              <Tooltip title="Refresh job list"  >
                <Button style={{ outline: 'none', height: '36px' }}
                  onClick={(e) => { setLoadingJobList(true); }}
                ><FontAwesomeIcon icon={faSyncAlt} className='fa-lg' />
                </Button>
              </Tooltip>
              <Divider orientation="vertical" flexItem />
              <Tooltip title="Reset to initial submit"  >
                <Button style={{ outline: 'none', height: '36px' }}
                  onClick={(e) => { setConfirm("reset the selected job to initial submit state"); }}
                ><FontAwesomeIcon icon={faFastBackward} className='fa-lg' />
                </Button>
              </Tooltip>
              <Tooltip title="Reset and keep input data"  >
                <Button style={{ outline: 'none', height: '36px' }}
                  onClick={(e) => {setConfirm("reset the selected job but keep the input data");; }}
                ><FontAwesomeIcon icon={faStepBackward} className='fa-lg' />
                </Button>
              </Tooltip>
              <Tooltip title="Stop/Cancel the selected running job"  >
                <Button style={{ outline: 'none', height: '36px' }}
                  onClick={(e) => { setConfirm("stop/cancel the selected job"); }}
                ><FontAwesomeIcon icon={faStop} className='fa-lg' />
                </Button>
              </Tooltip>
              <Tooltip title="Run selected analysis"  >
                <Button style={{ outline: 'none', height: '36px' }}
                  onClick={(e) => { setConfirm("run the selected job"); }}
                ><FontAwesomeIcon icon={faPlay} className='fa-lg' />
                </Button>
              </Tooltip>
              <Divider orientation="vertical" flexItem />
              <Tooltip title="Download results or other data"  >
                <Button style={{ outline: 'none', height: '36px' }}
                  onClick={(e) => { setDownloadOption({...downloadOption, isOpen: true}); }}
                ><FontAwesomeIcon icon={faDownload} className='fa-lg' />
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
              <Dialog
                open={downloadOption.isOpen}
                aria-labelledby="alert-dialog-title"
                aria-describedby="alert-dialog-description"
              >
                <DialogTitle id="alert-dialog-title">
                  {"Download Data Options"}
                </DialogTitle>
                <DialogContent>
                <FormControl>
                  <RadioGroup
                    aria-labelledby="demo-radio-buttons-group-label"
                    defaultValue="results"
                    name="radio-buttons-group"
                    value = {downloadOption.data_type}
                    onChange={(event) => {
                     setDownloadOption({...downloadOption, data_type:event.target.value});
                    }}
                  >
                    <FormControlLabel value="details" control={<Radio />} label="Details" />
                    <FormControlLabel value="unused" control={<Radio />} label="Unused Only" />
                    <FormControlLabel value="results" control={<Radio />} label="Results Only" />
                  </RadioGroup>
                </FormControl>

                </DialogContent>
                <DialogActions>
                  <Button style={{ color: 'black' }} onClick={() => { handleDownloadOpt(true);}} autoFocus>Download</Button>
                  <Button style={{ color: 'black' }} onClick={() => { handleDownloadOpt(false);}} > Cancel </Button>
                </DialogActions>
              </Dialog>
            </div>
            <Grid container className={classes.root} spacing={2}>
              <Grid item md={12} style={{ marginTop: '-30px' }}>
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
                            <SearchBar  {...props.searchProps} style={{ height: '26px', width: '180px' }} />
                          </Grid>
                        </Grid>
                        <BootstrapTable classes={classes.table}
                          cellEdit={cellEditFactory({
                            mode: 'dbclick', afterSaveCell: (oldValue, newValue, row, column) => {
                              handleRenameJob(row.job_id, newValue);
                            }
                          })}
                          {...props.baseProps}
                          rowClasses={classes.table_row}
                          selectRow={select_row}
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
        </div>
        <div class="pane_cont">
          <Allotment vertical defaultSizes={[50, 50]}>
            <div class="pane_cont">
              <h5 style={{marginLeft:'5px'}}>Parameters:</h5>
              <span style={{marginLeft:'5px'}}>{currentPara?.job_id}</span>
              <textarea value={currentPara?.parameter}
                readOnly={true}
                class="para_row"
                style={{ color: theme.palette.text.primary, background: theme.palette.background.default, width: '96%'}}>
              </textarea>
            </div>
            <div class="pane_cont">
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
                    {currentPara?.summary?.map(row => (
                      <tr key={row.item}>
                        <td>{row.item}</td>
                        <td>{row.cnt}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </Allotment>
        </div>
      </Allotment>
    </div>
  );
};
