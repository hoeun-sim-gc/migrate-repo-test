import React, { FC } from 'react';
import { LogoProps } from './../gc-logo/gc-logo.types';
import { MMCLogoHorizontal } from './mmc-logo-horizontal';
import { MMCLogoVertical } from './mmc-logo-vertical';

export const MMCLogo: FC<LogoProps> = ({ color, orient }) => {
  const isHorizontal = orient !== 'vertical';
  return (
    <div className="gcui-gc-logo">
      {isHorizontal ? <MMCLogoHorizontal color={color} /> : <MMCLogoVertical color={color} />}
    </div>
  );
};
