import { HTMLAttributes } from 'react';

export interface LogoProps extends HTMLAttributes<SVGElement> {
  color?: LogoColor;
  orient?: LogoOrientation;
}

export type LogoOrientation = 'horizontal' | 'vertical';
export type LogoColor = 'blue' | 'black' | 'white';
