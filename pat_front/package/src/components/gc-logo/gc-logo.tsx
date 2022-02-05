import React, { FC } from 'react';
import { LogoProps } from './gc-logo.types';
import { GcLogoHorizontal } from './gc-logo-horizontal';
import { GcLogoVertical } from './gc-logo-vertical';

export const GcLogo: FC<LogoProps> = ({ color, orient }) => {
  const isHorizontal = orient !== 'vertical';
  return (
    <div className="gcui-gc-logo">
      {isHorizontal ? <GcLogoHorizontal color={color} /> : <GcLogoVertical color={color} />}
    </div>
  );
};
