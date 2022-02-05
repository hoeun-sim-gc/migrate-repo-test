import React from 'react';
import { Meta, Story } from '@storybook/react';
import { MMCLogo, LogoProps } from '.';

const meta: Meta = {
  title: 'Components/Logos/MMC Logo',
  component: MMCLogo,
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

const Template: Story<LogoProps> = (args) => <MMCLogo {...args} />;
export const Logo = Template.bind({});
Logo.args = {};
