import React, { FC } from 'react';
import { LogoProps } from './gc-logo.types';
import logoColorMap from './gc-logo-colors';

export const GcLogoVertical: FC<LogoProps> = ({ color }) => {
  const fill = logoColorMap[color || 'blue'];

  return (
    <svg
      version="1.1"
      id="GcLogoVertical"
      xmlns="http://www.w3.org/2000/svg"
      x="0px"
      y="0px"
      viewBox="0 0 605.1079102 258.4257812"
    >
      <g>
        <g>
          <path
            fill={fill}
            d="M32.8056602,242.4209595c-6.5829754,0-12.329483-1.2872925-17.2386246-3.8618774 c-4.9109383-2.5736694-8.7328033-6.2224884-11.4660473-10.9482574C1.3663969,222.8868713,0,217.3134155,0,210.8899994 c0-6.3168945,1.4198841-11.8642883,4.2601018-16.6417236c2.8393188-4.7774506,6.794229-8.5062714,11.8642807-11.1873779 c5.0682545-2.6797485,10.9482555-4.0209808,17.6368561-4.0209808c3.7683792,0,7.2059479,0.3856506,10.3113556,1.1542511 c3.1054039,0.7703857,6.117321,1.7655182,9.0375443,2.9862976v10.3508911 c-6.3177872-2.9184113-12.9268341-4.3792114-19.8266869-4.3792114c-4.3531437,0-8.1754589,0.9034424-11.4660492,2.7071838 c-3.2919369,1.8055267-5.8525829,4.3270721-7.6837339,7.564621c-1.8316011,3.2384491-2.7471771,7.0068359-2.7471771,11.306488 c0,4.4592133,0.8881578,8.3080444,2.6676207,11.546051c1.7776642,3.2380066,4.3122406,5.719986,7.6041756,7.4450684 c3.2905922,1.7250671,7.1924629,2.5871582,11.7047195,2.5871582c4.1926804,0,8.2275963-0.5573425,12.102951-1.6720428v-13.2971954 H32.9647751v-9.714447h22.7729797v30.3371582C48.6769905,240.9358978,41.0332565,242.4209595,32.8056602,242.4209595z"
          />
          <path
            fill={fill}
            d="M83.0486145,242.4209595c-5.1496124,0-9.0910339-1.592041-11.8242798-4.7769928 c-2.7345963-3.184967-4.1009903-7.7498169-4.1009903-13.6954346v-28.9042358h10.749588v26.2766113 c0,4.0344696,0.7290497,6.9668274,2.1898346,8.7979736c1.4594345,1.8320618,3.8344498,2.7471771,7.1263885,2.7471771 c3.3962173,0,6.0238342-1.1407623,7.8828506-3.4240723c1.8572235-2.2815247,2.7867279-5.4143524,2.7867279-9.3948822v-25.0028076 h10.749588v45.9437408H98.2569733l-0.2391205-7.08638c-1.2216721,2.7067108-3.1323776,4.8048553-5.7330246,6.289917 C89.6837311,241.6775208,86.604393,242.4209595,83.0486145,242.4209595z"
          />
          <path
            fill={fill}
            d="M125.4968109,258.4257812c-1.2738037,0-2.6276169-0.1726074-4.0605392-0.5177917 c-1.4333649-0.3452148-2.7345886-0.72995-3.9018707-1.1542358v-9.5549011 c1.061203,0.3703613,2.2559052,0.7290344,3.5831985,1.0751343c1.3263931,0.3442993,2.5741272,0.5168915,3.7423096,0.5168915 c2.5480576,0,4.6183319-0.6238708,6.2108154-1.870697c1.5924835-1.2477417,3.1314697-3.6506195,4.6183319-7.2059479 l-19.8266907-44.6699371h10.9882584l14.4123459,32.4074249l13.4562988-32.4074249h10.4309235l-18.7915649,45.068161 c-2.0702667,4.9361115-4.1679535,8.7314606-6.2903595,11.3860474c-2.123764,2.6536865-4.3792114,4.4713593-6.768158,5.4538879 C130.9116058,257.9340515,128.3100586,258.4257812,125.4968109,258.4257812z"
          />
          <path
            fill={fill}
            d="M203.484314,242.4209595c-6.3703766,0-11.9712524-1.3007812-16.800827-3.9014282 c-4.8313904-2.5997467-8.5871887-6.2764282-11.2669373-11.0282593 c-2.6811066-4.7500305-4.0214386-10.3369751-4.0214386-16.7608337c0-4.6704712,0.7964783-8.9440613,2.3889465-12.8194122 c1.5924835-3.8744507,3.8479462-7.2185364,6.7681732-10.0326843c2.918869-2.8132477,6.3825073-4.9891357,10.3908997-6.52948 c4.0075073-1.5390015,8.426712-2.3089447,13.2576447-2.3089447c3.2905884,0,6.5555573,0.4117126,9.7940063,1.2342529 c3.2371063,0.8234253,6.0773315,1.9246368,8.5197601,3.3040771v10.5904694 c-3.184967-1.6450653-6.3438568-2.8788757-9.4753265-3.7027588c-3.1328278-0.822525-6.0791168-1.2342529-8.8384399-1.2342529 c-4.2475128,0-7.9624023,0.8895111-11.1473694,2.6676178c-3.184967,1.7790222-5.6674042,4.2735901-7.4450684,7.4846191 c-1.779007,3.2123871-2.6676178,6.9672852-2.6676178,11.2669373c0,4.4592133,0.8760223,8.3085022,2.6276245,11.5456085 c1.7520447,3.2388916,4.2326813,5.7208862,7.4450531,7.4450531c3.2110443,1.7259827,6.9933472,2.5880737,11.3464966,2.5880737 c2.8132477,0,5.8525848-0.4108276,9.1170959-1.2342529c3.2645264-0.8225403,6.3029633-1.8976746,9.1171112-3.2254181v10.2722473 c-2.8141479,1.3268433-5.8125763,2.388504-8.9975433,3.184967 C210.4115906,242.0227203,207.0401001,242.4209595,203.484314,242.4209595z"
          />
          <path
            fill={fill}
            d="M247.1964264,242.4209595c-4.7774353,0-8.6132355-1.3133698-11.5056-3.940979 c-2.8941498-2.6276245-4.3396454-6.1173248-4.3396454-10.4709167c0-3.1310272,0.769043-5.8521271,2.3089294-8.1615295 c1.5390015-2.3093872,3.7153473-4.1126709,6.5294952-5.4143372c2.8128052-1.3003235,6.1038361-1.9507141,9.8735657-1.9507141 c4.4052734,0,8.4401855,0.7699432,12.1029358,2.3089447v-0.1591187c0-3.8744507-1.0229797-6.8202972-3.0658264-8.8384247 c-2.0437622-2.0167847-5.1891785-3.0258636-9.4353485-3.0258636c-2.4959106,0-5.0561066,0.3982391-7.6837311,1.1942596 c-2.6276093,0.796463-5.0835419,1.9646454-7.365509,3.5036316v-9.4753265 c2.1759033-1.2203217,4.6444092-2.2554626,7.4050598-3.1054077c2.7597656-0.8486023,5.6795349-1.2738037,8.7588806-1.2738037 c6.79422,0,12.0233765,1.7515869,15.6861572,5.2552338c3.6627502,3.5031891,5.4938965,8.5197449,5.4938965,15.0487976v27.0726318 h-9.3157654l-0.1595764-6.6881561c-1.2738037,2.4954681-3.2919312,4.4722595-6.051239,5.9321442 C253.6724396,241.6910095,250.5930939,242.4209595,247.1964264,242.4209595z M241.3838501,227.3726044 c0,2.2824249,0.742981,4.0479584,2.2293854,5.2947998c1.4855042,1.2477264,3.5557861,1.870697,6.2108154,1.870697 c3.7683716,0,6.6886139-1.074234,8.7588654-3.2245178c2.070282-2.1502686,3.2371216-5.2678223,3.5036621-9.3562164 c-3.0258789-1.3798828-6.4773712-2.070282-10.3513641-2.070282c-3.3980255,0-5.9721527,0.6508484-7.7237396,1.9507141 C242.2598724,223.1394806,241.3838501,224.9832153,241.3838501,227.3726044z"
          />
          <path
            fill={fill}
            d="M283.3457336,240.9880371v-45.9437408h10.2722473l0.0791016,9.5553284 c1.2207642-3.1314697,3.0779724-5.6260529,5.5734558-7.4850616c2.4954834-1.8572235,5.307373-2.7867279,8.4411011-2.7867279 c0.8486023,0,1.8572083,0.0795441,3.0258484,0.2386627c1.1668396,0.1595612,2.096344,0.3721619,2.7867126,0.6373596v10.271347 c-0.8504028-0.26474-1.9650879-0.5038605-3.3440552-0.7164612c-1.3825989-0.2117004-2.6554871-0.3186798-3.8223267-0.3186798 c-3.9823303,0-7.020752,1.287735-9.1170959,3.8618774c-2.0981445,2.57547-3.1463013,6.3042908-3.1463013,11.1869202v21.499176 H283.3457336z"
          />
          <path
            fill={fill}
            d="M321.0888367,257.629303v-62.5850067h10.2713623l0.0791016,7.8032837 c1.6990051-2.9723511,3.9544678-5.2552338,6.768158-6.8477173s6.0247192-2.3884888,9.6348877-2.3884888 c4.1405334,0,7.8298035,1.0486145,11.0678101,3.1449585c3.2371216,2.0976868,5.7730408,4.963974,7.604187,8.599762 c1.8311462,3.6362305,2.7471924,7.8167725,2.7471924,12.5407257c0,3.4510651-0.5708313,6.6620941-1.7116089,9.634903 c-1.1425476,2.9728088-2.7345886,5.5734558-4.7778931,7.8028412c-2.0441895,2.2293854-4.4066162,3.9688416-7.0863953,5.215683 c-2.6815491,1.2468414-5.5617676,1.8707123-8.63974,1.8707123c-3.2380066,0-6.1838684-0.7164612-8.8384399-2.1493835 c-2.6546021-1.4338226-4.7770081-3.5040894-6.3699341-6.2108154v23.5685425H321.0888367z M344.8164978,232.7869568 c2.6536865,0,5.0026245-0.6238708,7.0468445-1.8715973c2.0433044-1.2468414,3.6497192-2.9853973,4.8174438-5.215683 c1.1668396-2.2293854,1.7520447-4.7769928,1.7520447-7.6437378c0-2.9184265-0.5852051-5.4938965-1.7520447-7.7232819 c-1.1677246-2.2298431-2.7741394-3.9679565-4.8174438-5.215683c-2.04422-1.2463837-4.393158-1.8711548-7.0468445-1.8711548 c-2.6545715,0-5.0161133,0.6247711-7.0863953,1.8711548c-2.0702515,1.2477264-3.6766663,2.9858398-4.8174438,5.215683 c-1.1416626,2.2293854-1.7115784,4.8048553-1.7115784,7.7232819c0,2.866745,0.5699158,5.4143524,1.7115784,7.6437378 c1.1407776,2.2302856,2.7471924,3.9688416,4.8174438,5.215683 C339.8003845,232.1630859,342.1619263,232.7869568,344.8164978,232.7869568z"
          />
          <path
            fill={fill}
            d="M401.9867249,242.4209595c-4.9900513,0-9.4227295-0.9286194-13.2972107-2.7867279 c-3.8753357-1.8572235-6.9272461-4.5909271-9.1566467-8.2010803c-2.2293701-3.6092682-3.3449707-8.0419769-3.3449707-13.2972107 c0-4.8300323,0.9960327-9.0775452,2.9862976-12.7402954s4.7374268-6.541626,8.2415161-8.6393127 c3.5032043-2.096344,7.5107117-3.1449585,12.023407-3.1449585c4.2996521,0,8.0545349,0.9429932,11.2664795,2.8262787 c3.2110291,1.8850861,5.6930237,4.5261841,7.4450684,7.9228516c1.7520447,3.3980255,2.6276245,7.3789978,2.6276245,11.9433899 c0,0.6373596-0.0269775,1.2621155-0.0800171,1.8716125c-0.0530396,0.6112823-0.1330566,1.5263977-0.2382202,2.7471771h-33.7616882 c0.6373596,3.8223114,2.3354492,6.8077087,5.0961304,8.9579773c2.7597656,2.1493835,6.4759827,3.2245178,11.1477966,3.2245178 c5.9447327,0,11.4121094-1.8572083,16.4030762-5.5734558v9.474884c-2.3894043,1.6459656-5.1096191,2.9602203-8.1615295,3.9418793 C408.130127,241.93013,405.0647278,242.4209595,401.9867249,242.4209595z M398.9608765,201.8919983 c-3.3980103,0-6.1586609,1.0490723-8.2810974,3.1454163c-2.123291,2.0976868-3.4510498,5.0300446-3.9814148,8.7984314h23.8876648 c0-0.6890411-0.0530396-1.299881-0.1591187-1.8311615c-0.53125-3.2910309-1.7789917-5.7990875-3.7423096-7.5246124 C404.7204285,202.7554474,402.1458435,201.8919983,398.9608765,201.8919983z"
          />
          <path
            fill={fill}
            d="M430.4131775,240.9880371v-45.9437408h10.3504639l0.0799866,7.3255005 c1.2738037-2.7593079,3.1975708-4.9091492,5.7730408-6.4494781c2.5736694-1.5390015,5.533905-2.3089447,8.8779907-2.3089447 c5.307373,0,9.395752,1.6855164,12.2625122,5.0561218c2.8658142,3.3714905,4.2996521,8.2150116,4.2996521,14.5314484v27.789093 h-10.7496033v-25.7983856c0-8.0149994-3.0788879-12.0233917-9.2366638-12.0233917 c-3.3970947,0-6.0651855,1.1281738-8.0024109,3.384079c-1.9381104,2.2568054-2.9062805,5.4282837-2.9062805,9.5153351v24.9223633 H430.4131775z"
          />
          <path
            fill={fill}
            d="M503.9055481,242.4209595c-5.4152527,0-9.423645-1.5650635-12.023407-4.6978912 c-2.601532-3.1310272-3.9023132-7.6697998-3.9023132-13.6154327v-19.2693329h-9.474884v-8.9975433h9.474884v-12.2625122 h10.7496033v12.2625122h14.9692383v8.9975433h-14.9692383v18.3937683c0,3.3440704,0.5699158,5.7721252,1.7124634,7.2850494 c1.1407776,1.5129242,2.9584351,2.2698364,5.4539185,2.2698364c1.6450806,0,3.3440857-0.2786713,5.0961304-0.8360138 c1.7520142-0.5573578,3.2649536-1.2072906,4.5387573-1.9507141v9.7931061 c-1.7520142,0.796463-3.6110535,1.4338226-5.5743408,1.9111633 C507.992157,242.1827393,505.9758301,242.4209595,503.9055481,242.4209595z"
          />
          <path
            fill={fill}
            d="M546.5038452,242.4209595c-4.9899902,0-9.4227295-0.9286194-13.2971802-2.7867279 c-3.8753662-1.8572235-6.9272461-4.5909271-9.1566772-8.2010803c-2.2293701-3.6092682-3.3449707-8.0419769-3.3449707-13.2972107 c0-4.8300323,0.9960327-9.0775452,2.9863281-12.7402954c1.9902344-3.6627502,4.7374268-6.541626,8.2415161-8.6393127 c3.5031738-2.096344,7.5106812-3.1449585,12.0233765-3.1449585c4.2996826,0,8.0545654,0.9429932,11.2664795,2.8262787 c3.2110596,1.8850861,5.6930542,4.5261841,7.4450684,7.9228516c1.7520752,3.3980255,2.6276245,7.3789978,2.6276245,11.9433899 c0,0.6373596-0.0269775,1.2621155-0.0800171,1.8716125c-0.0530396,0.6112823-0.1329956,1.5263977-0.2382202,2.7471771h-33.7616577 c0.6373291,3.8223114,2.3354492,6.8077087,5.0961304,8.9579773c2.7597656,2.1493835,6.4760132,3.2245178,11.1478271,3.2245178 c5.9447021,0,11.4121094-1.8572083,16.4030151-5.5734558v9.474884c-2.3894043,1.6459656-5.1095581,2.9602203-8.161499,3.9418793 C552.6472778,241.93013,549.5818481,242.4209595,546.5038452,242.4209595z M543.4780273,201.8919983 c-3.3980103,0-6.1586914,1.0490723-8.2810669,3.1454163c-2.1233521,2.0976868-3.4510498,5.0300446-3.9814453,8.7984314h23.8876953 c0-0.6890411-0.0530396-1.299881-0.1591187-1.8311615c-0.531311-3.2910309-1.7790527-5.7990875-3.7423096-7.5246124 C549.2375488,202.7554474,546.6629639,201.8919983,543.4780273,201.8919983z"
          />
          <path
            fill={fill}
            d="M574.9302979,240.9880371v-45.9437408h10.2713623l0.0791016,9.5553284 c1.2207642-3.1314697,3.0789185-5.6260529,5.5744019-7.4850616c2.4945679-1.8572235,5.307373-2.7867279,8.4401855-2.7867279 c0.8485718,0,1.8571777,0.0795441,3.0258179,0.2386627c1.1668701,0.1595612,2.0963745,0.3721619,2.7867432,0.6373596v10.271347 c-0.8494873-0.26474-1.9641724-0.5038605-3.3440552-0.7164612c-1.3817139-0.2117004-2.6555176-0.3186798-3.8223267-0.3186798 c-3.9814453,0-7.020752,1.287735-9.1171265,3.8618774c-2.097229,2.57547-3.1453857,6.3042908-3.1453857,11.1869202v21.499176 H574.9302979z"
          />
        </g>
        <path
          fill={fill}
          d="M245.8643646,31.8845177L193.2929535,1.599647c-3.7024384-2.1328628-9.7609863-2.1328628-13.4634094,0 L34.7977066,85.1482773l80.2570877-61.4886322L76.76091,1.5996506c-3.7024307-2.1328664-9.7609711-2.1328664-13.4634094,0 L10.7260923,31.8845215c-3.7024384,2.1328697-6.7317138,7.3739014-6.7290802,11.6422234v60.5439034 c-0.0026338,4.277359,3.0266418,9.5183945,6.7290802,11.651268l52.5714073,30.284874 c3.7024384,2.1328583,9.7609787,2.1328583,13.4634094,0l145.0318298-83.5486374l-80.2570801,61.4886322l38.2938843,22.0600052 c3.7024231,2.1328583,9.7609711,2.1328583,13.4634094,0l52.5714111-30.284874 c3.7024384-2.1328735,6.7317047-7.373909,6.7317047-11.6467438V43.5312691 C252.5960693,39.2584229,249.566803,34.0173874,245.8643646,31.8845177z"
        />
      </g>
    </svg>
  );
};
