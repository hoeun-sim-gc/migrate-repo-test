import React, { useState, useContext, useCallback, useRef } from 'react';
import {useHistory} from 'react-router-dom';

import Tooltip from '@mui/material/Tooltip';
import Link from '@mui/material/Link';

import { makeStyles, useTheme } from '@material-ui/core/styles';
import {Divider, Grid} from '@material-ui/core';

import { PulseLoader } from "react-spinners";
import BootstrapTable from 'react-bootstrap-table-next';
import paginationFactory from 'react-bootstrap-table2-paginator';
import ToolkitProvider, { Search } from 'react-bootstrap-table2-toolkit';
import 'bootstrap/dist/css/bootstrap.min.css';
import 'react-bootstrap-table-next/dist/react-bootstrap-table2.min.css';

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
  buttonLink: {
    color: 'inherit',
    '&:hover':{
      color: theme.palette.text.secondary,
    },
    textDecoration: 'inherit'
  },
}));

export default function HomePage(props) {
  const history = useHistory(); 
  const classes = useStyles();
  const theme = useTheme();

  const tableRef = useRef(null);

  const [user,] = useContext(UserContext);

  const [multiSel, setMultiSel]=useState(false);

  const [loadingJobList, setLoadingJobList] = useState(false);
  const [currentJob, setCurrentJob] = useState(null);
  const [jobList, setJobList] = useState([]);

  const [loadingJobPara, setLoadingJobPara] = useState(false);
  const [currentPara, setCurrentPara] = useState(null);

  const [loadingJobSum, setLoadingJobSum] = useState(false);
  const [currentSum, setCurrentSum] = useState([]);

  const [downloadingResults, setDownloadingResults] = useState(false);
  const [downloadingDatafile, setDownloadingDatafile] = useState(false);

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
    
    const request = '/api/job';
    fetch(request).then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(data => {
        data = data.filter(u => u.user_email.toLowerCase() === user.email.toLowerCase())
        data.forEach(job => {
          job.receive_time = convertTime(job.receive_time);
          job.update_time = convertTime(job.update_time);
        });
        data.sort((a, b) => (a.update_time > b.update_time) ? -1 : 1);
        setJobList(data);
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

    let request = 'api/result/' + lst.join('_');
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

  const handleGoJob = useCallback((job_id) => history.push('/job' + (job_id?'/'+job_id:'') ), [history]);

  //data file
  React.useEffect(() => {
    if (!downloadingDatafile) return;
    if (!currentJob) {
      setDownloadingDatafile(false);
      alert("No analysis is selected!");
      return;
    }

    let request = 'api/valid/' + currentJob.job_id;
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
        setDownloadingDatafile(false);
      });
    // eslint-disable-next-line
  }, [downloadingDatafile]);

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
        {(loadingJobList || loadingJobPara || downloadingResults || downloadingDatafile) &&
          <div className={classes.spinner}>
            <PulseLoader
              size={30}
              color={"#2BAD60"}
              loading={loadingJobList || loadingJobPara || downloadingResults || downloadingDatafile}
            />
          </div>
        }
        
        <div class='row'>
          <Tooltip title="Download Results">
            <Link className={classes.buttonLink} style={{padding:'10px 10px 10px 15px' }}
            component="button"
              onClick={(e) => { setDownloadingResults(true); }} >
              Results
            </Link>
          </Tooltip>
          <Tooltip title="Dowload Validation Data">
            <Link className={classes.buttonLink} style={{padding:'10px' }}
              component="button"
              onClick={(e) => { setDownloadingDatafile(true); }} >
              Validation
            </Link>
          </Tooltip>
          <Tooltip title="Populate allocated premium back to EDM">
            <Link className={classes.buttonLink} style={{padding:'10px' }}
            component="button"
              onClick={(e) => { alert("This option hasn't been implemented yet!"); }} >
              EDM
            </Link>
          </Tooltip>
          <Divider orientation="vertical" flexItem />
          <Tooltip title="New analysis copying this selected"  >
            <Link className={classes.buttonLink} style={{padding:'10px' }}
            component="button"
              onClick={(e) => { handleGoJob(currentJob?.job_id); }} >
              New
            </Link>
          </Tooltip>
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
                        <FormControlLabel control={<Checkbox checked={multiSel} onChange={handleSelChange} />} label="Multiple Selection" />
                        <SearchBar  {...props.searchProps} style={{ height: '26px' }} />
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
