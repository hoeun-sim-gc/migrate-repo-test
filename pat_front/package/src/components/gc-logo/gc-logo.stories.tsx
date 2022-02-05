import React from 'react';
import { Meta, Story } from '@storybook/react';
import { GcLogo, LogoProps } from '.';

const meta: Meta = {
  title: 'Components/Logos/GC Logo',
  component: GcLogo,
  argTypes: {
    color: {
      control: {
        type: 'select',
        options: ['blue', 'black', 'white'],
      },
    },
    orient: {
      control: {
        type: 'select',
        options: ['horizontal', 'vertical'],
      },
    },
  },
  parameters: {
    controls: { expanded: true },
  },
};

export default meta;

const Template: Story<LogoProps> = (args) => <GcLogo {...args} />;
export const Logo = Template.bind({});
Logo.args = {};
