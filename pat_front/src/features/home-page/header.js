const columns = [{
    dataField: 'job_id',
    text: 'Job_id',
    sort: true,
    style: { overflow: 'hidden' },
    headerStyle: { width: '60px'}
  }, {
    dataField: 'job_name',
    text: 'Job_Name',
    sort: true,
    style: { overflow: 'hidden' },
    headerStyle: { width: '120px'}
  },
  {
    dataField: 'receive_time',
    text: 'Receive',
    sort: true,
    style: { overflow: 'hidden' },
    headerStyle: { width: '110px'}
  }, {
    dataField: 'update_time',
    text: 'Update',
    sort: true,
    style: { overflow: 'hidden' },
    headerStyle: { width: '110px'}
  }, {
    dataField: 'status',
    text: 'Status',
    sort: true,
    style: { overflow: 'hidden' },
    headerStyle: { width: '55px'}
  }, {
    dataField: 'user_name',
    text: 'Analyst',
    sort: true,
    style: { overflow: 'hidden' },
    headerStyle: { width: '80px'}
  }];

  export default columns;