import React, { useState, useContext, useCallback } from 'react';
import {useHistory} from 'react-router-dom';

import { makeStyles, useTheme } from '@material-ui/core/styles';
import {Grid,Button} from '@material-ui/core';

import { PulseLoader } from "react-spinners";
import BootstrapTable from 'react-bootstrap-table-next';
import paginationFactory from 'react-bootstrap-table2-paginator';
import ToolkitProvider, { Search } from 'react-bootstrap-table2-toolkit';
import 'bootstrap/dist/css/bootstrap.min.css';
import 'react-bootstrap-table-next/dist/react-bootstrap-table2.min.css';

import useClippy from 'use-clippy';

import { UserContext } from "../../app/user-context";
import columns from './header';
import WbMenu from '../../app/menu';
import { convertTime } from '../../app/theme'

import './index.css';

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
  summary: {
    color: theme.palette.text.primary,
    marginTop: 10,
    marginBottom: 10,
    overflow: "auto",
  },
}));

export default function HomePage(props) {
  const history = useHistory(); 
  const classes = useStyles();
  const theme = useTheme();

  const [user,] = useContext(UserContext);

  const [loadingJobList, setLoadingJobList] = useState(false);
  const [selectedJob, setSelectedJob] = useState(null);
  const [jobList, setJobList] = useState([]);

  const [loadingJobPara, setLoadingJobPara] = useState(false);
  const [selectedPara, setSelectedPara] = useState(null);

  const [loadingJobSum, setLoadingJobSum] = useState(false);
  const [selectedSum, setSelectedSum] = useState([]);

  const [downloadingResults, setDownloadingResults] = useState(false);
  const [downloadingDatafile, setDownloadingDatafile] = useState(false);

  const [clipboard, setClipboard] = useClippy();

  React.useEffect(() => {
    setLoadingJobList(true);

    const interval = setInterval(() => setLoadingJobList(true), 60000);
    return () => {
      clearInterval(interval);
    };

    return () => {
      clearInterval(interval);
    };
  }, []);

  //job list
  React.useEffect(() => {
    if (!loadingJobList) return;
    
    const request = '/api/Jobs';
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
    setSelectedPara('');
    setLoadingJobPara(true);

    setSelectedSum([]);
    setLoadingJobSum(true);
  }, [selectedJob]);

  //job para
  React.useEffect(() => {
    if (!loadingJobPara) return;
    if (!selectedJob) {
      setLoadingJobPara(false);
      return;
    }

    const request = '/api/Jobs/' + selectedJob.job_id+'/Para';
    fetch(request).then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(data => {
        setSelectedPara(JSON.stringify(data, null,'  '));
      })
      .catch(error => {
        console.log(error);
      })
      .then(() => {
        setLoadingJobPara(false);
      });
    // eslint-disable-next-line
  }, [loadingJobPara, selectedJob]);

  //job summary
  React.useEffect(() => {
    if (!loadingJobSum) return;
    if (!selectedJob) {
      setLoadingJobSum(false);
      return;
    }

    const request = '/api/Jobs/' + selectedJob.job_id;
    fetch(request).then(response => {
      if (response.ok) {
        return response.json();
      }
      throw new TypeError("Oops, we haven't got data!");
    })
      .then(data => {
        setSelectedSum(data);
      })
      .catch(error => {
        console.log(error);
      })
      .then(() => {
        setLoadingJobSum(false);
      });
    // eslint-disable-next-line
  }, [loadingJobSum, selectedJob]);

  //results
  React.useEffect(() => {
    if (!downloadingResults) return;
    if (!selectedJob || selectedJob.status !== 'finished') {
      setDownloadingResults(false);
      alert("No analysis is selected!");
      return;
    }

    let request = 'api/Jobs/' + selectedJob.job_id+"/Result";
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
        a.download = "pat_result_" + selectedJob.job_id + ".zip";
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

  const handleGoJob = useCallback((job_id) => history.push('/job/' + job_id), [history]);

  //data file
  React.useEffect(() => {
    if (!downloadingDatafile) return;
    if (!selectedJob) {
      setDownloadingDatafile(false);
      alert("No analysis is selected!");
      return;
    }

    let request = 'api/Jobs/' + selectedJob.job_id+"/Validation";
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
        a.download = "pat_validation" + selectedJob.job_id + ".zip";
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

  const options = {
    // pageStartIndex: 0,
    sizePerPage: 19, 
    hideSizePerPage: true,
    hidePageListOnlyOnePage: true,
    // sizePerPageList: [5,10,15,25,30,40,50],
    showTotal: true,
  };

  const selectRow = {
    mode: 'radio',
    clickToSelect: true,
    style: { backgroundColor: theme.palette.action.selected, fontWeight: 'bold' },
    onSelect: (row, isSelect) => {
      if (isSelect) {
        setSelectedJob(row);
      }
    }
  };

  const { SearchBar } = Search;

  return (
    <div>
      {(loadingJobList || loadingJobPara || downloadingResults || downloadingDatafile) &&
        <div className={classes.spinner}>
          <PulseLoader
            size={30}
            color={"#2BAD60"}
            loading={loadingJobList || loadingJobPara || downloadingResults || downloadingDatafile}
          />
        </div>
      }
      <WbMenu header="Selected Analysis" items={[
          { text: 'Download Results', onClick: () => { setDownloadingResults(true) } },
          { text: 'Dowload Validation', onClick: () => { setDownloadingDatafile(true) } },
          { text: 'Divider' },
          { text: 'Populate to EDM', onClick: () => {alert("Haven't implemented yet!") } },
          { text: 'Divider' },
          { text: 'New Analyis Copy This', onClick: () => {handleGoJob(selectedJob?.job_id) } },
        ]} />
      <Grid container className={classes.root} spacing={2}>
        <Grid item md={8} style={{ marginTop: '-28px' }}>
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
                    <Grid item md={6} container justify='flex-end'>
                      <SearchBar  {...props.searchProps} style={{height:'26px'}} />
                    </Grid>
                  </Grid>
                  <BootstrapTable classes={classes.table}
                    //ref={tableRef} 
                    {...props.baseProps}
                    rowClasses={classes.table_row}
                    selectRow={selectRow}
                    pagination={paginationFactory(options)}
                    striped
                    hover
                    condensed
                  />
                </div>
              )
            }
          </ToolkitProvider>
        </Grid>
        <Grid item md={4} style={{ marginTop: '-30px' }}>
            <div>
              <Grid container>
                <Grid item md={8}>
                  <h5>Parameters:</h5>
                </Grid>
                <Grid item container md={4} justify='flex-end' >
                  <Button onClick={(e) => { setClipboard(selectedPara); alert("Analysis parameters have been copied to Clipboard!"); }} >Copy</Button>
                </Grid>
              </Grid>
              <div>
                <pre className={classes.para} style={{height:'280px', border: '1px solid gray' }} >{selectedPara}</pre>
              </div>
              <Grid container>
                <Grid item md={8}>
                  <h5>Summary:</h5>
                </Grid>
                {/* <Grid item container md={4} justify='flex-end'>
                  <Button onClick={(e) => { setClipboard(selectedSum); alert("Analysis summary been copied to Clipboard!"); }} >Copy</Button>
                </Grid> */}
              </Grid>
                  <table style={{width:'100%', border: '1px solid gray'}} className={classes.summary} >
                    <thead>
                      <tr>
                        <th>Item</th>
                        <th>Count</th>
                      </tr>
                    </thead>
                    <tbody>
                      {selectedSum.map(row => (
                        <tr key={row.item}>
                          <td>{row.item}</td>
                          <td>{row.cnt}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
            </div>
          </Grid>
      </Grid>
    </div>
  );
};
