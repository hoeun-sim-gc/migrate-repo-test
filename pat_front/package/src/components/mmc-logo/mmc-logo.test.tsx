import React from 'react';
import { render } from 'react-dom';
import { act } from 'react-dom/test-utils';
import { MMCLogo } from './mmc-logo';

let container: HTMLDivElement;

beforeEach(() => {
  container = document.createElement('div');
  document.body.appendChild(container);
});

describe('MMCLogo', () => {
  it('renders blue horizontal logo by default', () => {
    act(() => {
      render(<MMCLogo />, container);
    });
    expect(container.querySelector('svg#MmcLogoHorizontalComponent')).not.toBeNull();
    expect(container.querySelector('svg g path[fill="#002c77"]')).not.toBeNull();
  });

  it('renders both orientations', () => {
    act(() => {
      render(<MMCLogo />, container);
    });
    expect(container.querySelector('svg#MmcLogoHorizontalComponent')).not.toBeNull();

    act(() => {
      render(<MMCLogo orient="horizontal" />, container);
    });
    expect(container.querySelector('svg#MmcLogoHorizontalComponent')).not.toBeNull();

    act(() => {
      render(<MMCLogo orient="vertical" />, container);
    });
    expect(container.querySelector('svg#MmcLogoVerticalComponent')).not.toBeNull();
  });

  it('renders all colors', () => {
    act(() => {
      render(<MMCLogo color="black" />, container);
    });
    expect(container.querySelector('svg g path[fill="#000000"]')).not.toBeNull();

    act(() => {
      render(<MMCLogo color="blue" />, container);
    });
    expect(container.querySelector('svg g path[fill="#002c77"]')).not.toBeNull();

    act(() => {
      render(<MMCLogo color="white" />, container);
    });
    expect(container.querySelector('svg g path[fill="#ffffff"]')).not.toBeNull();
  });
});
