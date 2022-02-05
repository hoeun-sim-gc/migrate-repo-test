import React, { FC } from 'react';
import { LogoProps } from './../gc-logo/gc-logo.types';
import logoColorMap from './../gc-logo/gc-logo-colors';

export const MMCLogoHorizontal: FC<LogoProps> = ({ color }) => {
  const fill = logoColorMap[color || 'blue'];

  return (
    <svg
      version="1.1"
      id="MmcLogoHorizontalComponent"
      xmlns="http://www.w3.org/2000/svg"
      x="0px"
      y="0px"
      viewBox="0 0 1790.286377 147.6064301"
    >
      <g>
        <g>
          <path
            fill={fill}
            d="M314.3424988,139.7680511V8.2885647h25.6035156l45.3261719,70.5839844l45.8457031-70.5839844h24.9111328
            v131.4794922h-23.8740234v-95.150383l-47.0556641,72.8339844l-46.8828125-72.6601562v94.9765549H314.3424988z"
          />
          <path
            fill={fill}
            d="M511.1238708,142.8813324c-10.3798828,0-18.7138672-2.8535156-24.9980469-8.5625
            c-6.2880859-5.7089844-9.4287109-13.291008-9.4287109-22.7499924c0-6.8027344,1.6708984-12.7148438,5.0166016-17.7324219
            c3.34375-5.0175781,8.0722656-8.9355469,14.1865234-11.7636719c6.1113281-2.8251953,13.2617188-4.2382812,21.452179-4.2382812
            c9.5712891,0,18.3378906,1.6728516,26.2958984,5.0166016v-0.3457031c0-8.4179688-2.2226562-14.8183594-6.6611328-19.203125
            c-4.4404297-4.3818359-11.2744141-6.5742188-20.5-6.5742188c-5.4228821,0-10.9853821,0.8652344-16.6943665,2.5947266
            c-5.7089844,1.7304688-11.0449219,4.2685547-16.0029297,7.6123047V46.3481369
            c4.7275391-2.6513672,10.0908203-4.9003906,16.0888672-6.7470703c5.9960938-1.84375,12.3398743-2.7675781,19.030304-2.7675781
            c14.7617188,0,26.1230469,3.8056641,34.0810547,11.4179688c7.9580078,7.6113281,11.9365234,18.5107422,11.9365234,32.6962891
            v58.8203049h-20.2402344l-0.3466797-14.5312424c-2.7675781,5.4218674-7.1523438,9.7167892-13.1474609,12.8886642
            C525.1942139,141.2953949,518.5037842,142.8813324,511.1238708,142.8813324z M498.4949646,110.1860275
            c0,4.9589844,1.6142578,8.7949219,4.84375,11.5039062c3.2275391,2.7109375,7.7255859,4.0644531,13.4941711,4.0644531
            c8.1875,0,14.5322266-2.3339844,19.0302734-7.0058594s7.0332031-11.4453125,7.6123047-20.328125
            c-6.5742188-2.9980469-14.0732422-4.4980469-22.4902344-4.4980469c-7.3828125,0-12.9756165,1.4140625-16.7812805,4.2382812
            C500.3982849,100.9887619,498.4949646,104.9946213,498.4949646,110.1860275z"
          />
          <path
            fill={fill}
            d="M589.6708984,139.7680511V39.9467697h22.3173828l0.1728516,20.7607422
            c2.6513672-6.8037109,6.6875-12.2236328,12.1103516-16.2626953c5.4189453-4.0351562,11.53125-6.0546875,18.3378906-6.0546875
            c1.84375,0,4.0351562,0.1728516,6.5742188,0.5185547c2.5351562,0.3466797,4.5546875,0.8085938,6.0546875,1.3847656v22.3164062
            c-1.8466797-0.5751953-4.2685547-1.0947266-7.2666016-1.5566406c-3-0.4599609-5.7675781-0.6923828-8.3037109-0.6923828
            c-8.6494141,0-15.2539062,2.7978516-19.8085938,8.390625c-4.5576172,5.5957031-6.8330078,13.6972656-6.8330078,24.3056641
            v46.7109299H589.6708984z"
          />
          <path
            fill={fill}
            d="M702.2237549,142.8813324c-3.5761719,0-7.46875-0.4316406-11.6767578-1.296875
            c-4.2119141-0.8652344-8.2480469-2.046875-12.1103516-3.546875c-3.8652344-1.4960938-7.2392578-3.1699219-10.1201172-5.015625
            v-20.4140549c10.2636719,7.265625,21.21875,10.8984375,32.8691406,10.8984375
            c6.8037109,0,11.7646484-0.8359375,14.8779297-2.5078125c3.1142578-1.671875,4.671875-4.296875,4.671875-7.8730469
            c0-3.1132812-1.4140625-5.6484375-4.2392578-7.6113281c-2.8271484-1.9589844-8.2763672-4.2089844-16.3486328-6.7480469
            c-11.3037109-3.5722656-19.2890625-7.7246094-23.9599609-12.4550781
            c-4.6708984-4.7275391-7.0068359-11.1289062-7.0068359-19.203125c0-9.3417969,3.4003906-16.7216797,10.2070312-22.1435547
            c6.8037109-5.4199219,16.0888672-8.1308594,27.8535156-8.1308594c4.4980469,0,9.6279297,0.7783203,15.3964844,2.3349609
            c5.765625,1.5576172,10.7832031,3.546875,15.0507812,5.96875v20.4140625
            c-3.6923828-2.7685547-8.3896484-5.0439453-14.0986328-6.8339844
            c-5.7089844-1.7861328-10.9863281-2.6816406-15.8300781-2.6816406c-10.8417969,0-16.2617188,3.2871094-16.2617188,9.8613281
            c0,1.9628906,0.4892578,3.6064453,1.4707031,4.9306641c0.9785156,1.3271484,2.7949219,2.625,5.4492188,3.8925781
            c2.6513672,1.2705078,6.4580078,2.7109375,11.4179688,4.3251953c8.5341797,2.7675781,15.2509766,5.6816406,20.1542969,8.7363281
            c4.9013672,3.0566406,8.4199829,6.546875,10.5547485,10.4667969c2.1328125,3.921875,3.2011719,8.7089844,3.2011719,14.359375
            c0,9.2285156-3.781311,16.5800705-11.3340454,22.056633C724.8575439,140.141098,714.7940674,142.8813324,702.2237549,142.8813324
            z"
          />
          <path
            fill={fill}
            d="M763.9885864,139.7680511V8.2885647h23.3554688v46.0175781
            c2.8808594-5.5361328,7.0332031-9.8310547,12.4560547-12.8886719c5.4199219-3.0546875,11.6474609-4.5839844,18.6835938-4.5839844
            c11.53125,0,20.4140625,3.6621094,26.6425781,10.9853516c6.2275391,7.3251953,9.3417969,17.8486328,9.3417969,31.5722656
            v60.3769455h-23.3554688V83.716301c0-17.4140625-6.6904297-26.1230469-20.0673828-26.1230469
            c-7.3828125,0-13.1777344,2.4511719-17.3867188,7.3525391c-4.2119141,4.9033203-6.3144531,11.7939453-6.3144531,20.6738281
            v54.1484299H763.9885864z"
          />
          <path
            fill={fill}
            d="M882.317688,139.7680511V8.2885647h25.6035156l45.3261719,70.5839844l45.8457031-70.5839844h24.9120483
            v131.4794922h-23.874939v-95.150383l-47.0546875,72.8339844l-46.8837891-72.6601562v94.9765549H882.317688z"
          />
          <path
            fill={fill}
            d="M1097.6047363,142.8813324c-10.7265625,0-20.0683594-2.1347656-28.0273438-6.4003906
            c-7.9570312-4.265625-14.0996094-10.3496017-18.4238281-18.2519455
            c-4.3242188-7.8984375-6.4882812-17.1542969-6.4882812-27.765625c0-10.7265625,2.2207031-20.0957031,6.6621094-28.1123047
            c4.4375-8.015625,10.609375-14.2734375,18.5097656-18.7705078c7.8984375-4.4980469,17.0410156-6.7470703,27.421875-6.7470703
            c5.1894531,0,9.9746094,0.6347656,14.3574219,1.9023438c4.3828125,1.2705078,8.5351562,2.7685547,12.4570312,4.4980469v21.625
            c-3.8066406-2.3056641-7.9316406-4.0654297-12.3691406-5.2763672c-4.4414062-1.2109375-8.6796875-1.8164062-12.7167969-1.8164062
            c-9.3417969,0-16.8105469,2.8544922-22.4023438,8.5634766c-5.5957031,5.7089844-8.390625,13.4667969-8.390625,23.2685547
            c0,10.265625,2.7949219,18.2519531,8.390625,23.9609375c5.5917969,5.7089844,13.3476562,8.5625,23.2675781,8.5625
            c4.0371094,0,7.8984375-0.4882812,11.5917969-1.4707031c3.6894531-0.9785156,7.8984375-2.5078125,12.6289062-4.5839844v20.587883
            c-3.6933594,1.8457031-7.7578125,3.34375-12.1972656,4.4980469
            C1107.4348145,142.3032074,1102.677002,142.8813324,1097.6047363,142.8813324z"
          />
          <path
            fill={fill}
            d="M1148.9836426,139.7680511V8.2885647h24.2207031v109.8544922h60.5488281v21.6249924H1148.9836426z"
          />
          <path
            fill={fill}
            d="M1300.7023926,142.8813324c-10.8417969,0-20.4726562-2.0175781-28.890625-6.0546875
            c-8.4199219-4.0351562-15.0507812-9.9746017-19.8945312-17.8183517c-4.84375-7.8417969-7.2675781-17.4726562-7.2675781-28.890625
            c0-10.4941406,2.1640625-19.7226562,6.4882812-27.6806641s10.2929688-14.2128906,17.90625-18.7705078
            c7.6113281-4.5546875,16.3183594-6.8330078,26.1230469-6.8330078c9.3417969,0,17.5,2.0488281,24.4785156,6.140625
            c6.9765625,4.0957031,12.3691406,9.8339844,16.1757812,17.2138672
            c3.8066406,7.3828125,5.7089844,16.0322266,5.7089844,25.9492188c0,1.3847656-0.0585938,2.7421875-0.1738281,4.0664062
            c-0.1152344,1.328125-0.2890625,3.3164062-0.5175781,5.96875h-73.3535156
            c1.3847656,8.3046875,5.0742188,14.7910156,11.0722656,19.4628906
            c5.9960938,4.6699219,14.0703125,7.0058594,24.2207031,7.0058594c12.9160156,0,24.7949219-4.0351562,35.6386719-12.109375
            v20.5859299c-5.1914062,3.5761719-11.1015625,6.4316406-17.7324219,8.5644531
            C1314.0500488,141.8149261,1307.3898926,142.8813324,1300.7023926,142.8813324z M1294.1281738,54.8246994
            c-7.3828125,0-13.3808594,2.2792969-17.9921875,6.8339844c-4.6132812,4.5576172-7.4980469,10.9287109-8.6503906,19.1162109
            h51.9003906c0-1.4970703-0.1152344-2.8242188-0.3457031-3.9785156
            c-1.1542969-7.1503906-3.8652344-12.5996094-8.1308594-16.3486328
            C1306.6418457,56.700676,1301.0480957,54.8246994,1294.1281738,54.8246994z"
          />
          <path
            fill={fill}
            d="M1362.4621582,139.7680511V39.9467697h22.4882812l0.1738281,15.9160156
            c2.7675781-5.9951172,6.9472656-10.6660156,12.5429688-14.0126953c5.5917969-3.34375,12.0234375-5.0166016,19.2890625-5.0166016
            c11.53125,0,20.4140625,3.6621094,26.6425781,10.9853516c6.2265625,7.3251953,9.3417969,17.8486328,9.3417969,31.5722656
            v60.3769455h-23.3554688V83.716301c0-17.4140625-6.6894531-26.1230469-20.0683594-26.1230469
            c-7.3808594,0-13.1777344,2.4511719-17.3867188,7.3525391c-4.2109375,4.9033203-6.3144531,11.7939453-6.3144531,20.6738281
            v54.1484299H1362.4621582z"
          />
          <path
            fill={fill}
            d="M1477.6789551,139.7680511V39.9467697h22.4882812l0.1738281,15.9160156
            c2.7675781-5.9951172,6.9472656-10.6660156,12.5429688-14.0126953c5.5917969-3.34375,12.0234375-5.0166016,19.2890625-5.0166016
            c11.53125,0,20.4140625,3.6621094,26.6425781,10.9853516c6.2265625,7.3251953,9.3417969,17.8486328,9.3417969,31.5722656
            v60.3769455h-23.3554688V83.716301c0-17.4140625-6.6894531-26.1230469-20.0683594-26.1230469
            c-7.3808594,0-13.1777344,2.4511719-17.3867188,7.3525391c-4.2109375,4.9033203-6.3144531,11.7939453-6.3144531,20.6738281
            v54.1484299H1477.6789551z"
          />
          <path
            fill={fill}
            d="M1621.2648926,142.8813324c-10.3789062,0-18.7128906-2.8535156-24.9980469-8.5625
            c-6.2871094-5.7089844-9.4296875-13.291008-9.4296875-22.7499924c0-6.8027344,1.671875-12.7148438,5.0175781-17.7324219
            c3.34375-5.0175781,8.0722656-8.9355469,14.1855469-11.7636719c6.1132812-2.8251953,13.2617188-4.2382812,21.453125-4.2382812
            c9.5722656,0,18.3378906,1.6728516,26.2949219,5.0166016v-0.3457031c0-8.4179688-2.2207031-14.8183594-6.6601562-19.203125
            c-4.4414062-4.3818359-11.2734375-6.5742188-20.5-6.5742188c-5.421875,0-10.9863281,0.8652344-16.6953125,2.5947266
            c-5.7089844,1.7304688-11.0449219,4.2685547-16.0019531,7.6123047V46.3481369
            c4.7285156-2.6513672,10.0917969-4.9003906,16.0898438-6.7470703c5.9941406-1.84375,12.3398438-2.7675781,19.0292969-2.7675781
            c14.7617188,0,26.1230469,3.8056641,34.0800781,11.4179688c7.9589844,7.6113281,11.9375,18.5107422,11.9375,32.6962891
            v58.8203049h-20.2402344l-0.3457031-14.5312424c-2.7695312,5.4218674-7.1523438,9.7167892-13.1484375,12.8886642
            C1635.3352051,141.2953949,1628.6437988,142.8813324,1621.2648926,142.8813324z M1608.6359863,110.1860275
            c0,4.9589844,1.6132812,8.7949219,4.84375,11.5039062c3.2285156,2.7109375,7.7265625,4.0644531,13.4941406,4.0644531
            c8.1875,0,14.53125-2.3339844,19.0292969-7.0058594s7.0351562-11.4453125,7.6132812-20.328125
            c-6.5742188-2.9980469-14.0722656-4.4980469-22.4902344-4.4980469c-7.3828125,0-12.9746094,1.4140625-16.78125,4.2382812
            C1610.5383301,100.9887619,1608.6359863,104.9946213,1608.6359863,110.1860275z"
          />
          <path
            fill={fill}
            d="M1699.8078613,139.7680511V39.9467697h22.4882812l0.1738281,15.9160156
            c2.7675781-5.9951172,6.9472656-10.6660156,12.5429688-14.0126953c5.5917969-3.34375,12.0234375-5.0166016,19.2890625-5.0166016
            c11.53125,0,20.4140625,3.6621094,26.6425781,10.9853516c6.2265625,7.3251953,9.3417969,17.8486328,9.3417969,31.5722656
            v60.3769455h-23.3554688V83.716301c0-17.4140625-6.6894531-26.1230469-20.0683594-26.1230469
            c-7.3808594,0-13.1777344,2.4511719-17.3867188,7.3525391c-4.2109375,4.9033203-6.3144531,11.7939453-6.3144531,20.6738281
            v54.1484299H1699.8078613z"
          />
        </g>
        <path
          fill={fill}
          d="M241.8673553,31.8845177L189.2959442,1.599647c-3.7024384-2.1328628-9.7609863-2.1328628-13.4634094,0
          L30.8006973,85.1482773l80.2570877-61.4886322L72.7639008,1.5996506c-3.7024307-2.1328664-9.7609711-2.1328664-13.4634094,0
          L6.7290821,31.8845215c-3.7024386,2.1328697-6.7317142,7.3739014-6.7290802,11.6422234v60.5439034
          c-0.0026338,4.277359,3.0266418,9.5183945,6.7290802,11.651268l52.5714111,30.284874
          c3.7024384,2.1328583,9.7609787,2.1328583,13.4634094,0l145.0318298-83.5486374l-80.2570801,61.4886322l38.2938843,22.0600052
          c3.7024231,2.1328583,9.7609711,2.1328583,13.4634094,0l52.5714111-30.284874
          c3.7024384-2.1328735,6.7317047-7.373909,6.7317047-11.6467438V43.5312691
          C248.5990601,39.2584229,245.5697937,34.0173874,241.8673553,31.8845177z"
        />
      </g>
    </svg>
  );
};
