export const blending_columns = [
  {
    dataField: 'id',
    text: 'Rating Group',
    sort: true,
    style: { overflow: 'hidden' },
    headerStyle: { width: '80px'}
  },{
    dataField: 'name',
    text: 'Occupancy Name',
    sort: true,
    style: { overflow: 'hidden' },
    headerStyle: { width: '110px'},
    editable: false
  },{
    dataField: 'weight',
    text: 'Weight',
    sort: true,
    style: { overflow: 'hidden' },
    headerStyle: { width: '80px'},
    type:'number',
    validator: (newValue, row, column) => {
      if(newValue>=0) return;
      else return {
        valid: false,
        message: 'Weight is not in the range'
      }
    }
  }];

export const psold_rg = [
  {id:1,name:'Apartment/Condo under 10 units', weight:0},
  {id:2,name:'Apartment/Condo over 10 units', weight:0},
  {id:3,name:'Dwelling', weight:0},
  {id:4,name:'Group Institutional Housing', weight:0},
  {id:5,name:'Hospitals and Nursing Homes', weight:0},
  {id:6,name:'Hotels and Motels - With Restaurant', weight:0},
  {id:7,name:'Hotels and Motels - Other', weight:0},
  {id:8,name:'Entertainment and Recreation', weight:0},
  {id:9,name:'Restaurants and Bars', weight:0},
  {id:10,name:'Emergency Services', weight:0},
  {id:11,name:'Government Services', weight:0},
  {id:12,name:'Churches', weight:0},
  {id:13,name:'Schools', weight:0},
  {id:14,name:'Offices and Banks', weight:0},
  {id:15,name:'Other Mercantiles - Retail/Wholesale', weight:0},
  {id:16,name:'Other Mercantiles - Other', weight:0},
  {id:17,name:'Gasoline Stations', weight:0},
  {id:18,name:'Auto repair', weight:0},
  {id:19,name:'Parking', weight:0},
  {id:20,name:'Billboards', weight:0},
  {id:21,name:'Personal and Repair Services', weight:0},
  {id:22,name:'Buildings Under Construction', weight:0},
  {id:23,name:'Air/Airplane Hangars', weight:0},
  {id:24,name:'Storage', weight:0},
  {id:25,name:'Agricultural - Greenhouses', weight:0},
  {id:26,name:'Agricultural - Grain Elevators', weight:0},
  {id:27,name:'Food Processing - Other', weight:0},
  {id:28,name:'Food Processing - Severe', weight:0},
  {id:29,name:'General Indu/Metal Manufacturing', weight:0},
  {id:30,name:'Chemical Manufacturing', weight:0},
  {id:31,name:'Light Manufacturing - Printing', weight:0},
  {id:32,name:'Light Manufacturing - Other', weight:0},
  {id:33,name:'Heavy Manufacturing - Wood', weight:0},
  {id:34,name:'Heavy Manufacturing - Other', weight:0},
  {id:35,name:'Severe Manufacturing - Petroleum', weight:0},
  {id:36,name:'Highly Protected Risks - Low', weight:0},
  {id:37,name:'Highly Protected Risks - Medium', weight:0},
  {id:38,name:'Highly Protected Risks - Heavy', weight:0},
  {id:39,name:'All (Excl. HPR)', weight:0}
];

export const blending_types = [
  {id: 'no_blending', name: 'No Blening'},
  {id: 'missing_invalid', name: 'Only Apply to Missing/Invalid Rating Groups'},
  {id: 'all_blending', name: 'Apply to All Rating Groups'}
];