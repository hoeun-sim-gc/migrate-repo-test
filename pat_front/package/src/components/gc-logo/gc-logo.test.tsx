import React from 'react';
import { render } from 'react-dom';
import { act } from 'react-dom/test-utils';
import { GcLogo } from './gc-logo';

let container: HTMLDivElement;

beforeEach(() => {
  container = document.createElement('div');
  document.body.appendChild(container);
});

describe('GcLogo', () => {
  it('renders blue horizontal logo by default', () => {
    act(() => {
      render(<GcLogo />, container);
    });
    expect(container.querySelector('svg#GcLogoHorizontal')).not.toBeNull();
    expect(container.querySelector('svg g path[fill="#002c77"]')).not.toBeNull();
  });

  it('renders both orientations', () => {
    act(() => {
      render(<GcLogo />, container);
    });
    expect(container.querySelector('svg#GcLogoHorizontal')).not.toBeNull();

    act(() => {
      render(<GcLogo orient="horizontal" />, container);
    });
    expect(container.querySelector('svg#GcLogoHorizontal')).not.toBeNull();

    act(() => {
      render(<GcLogo orient="vertical" />, container);
    });
    expect(container.querySelector('svg#GcLogoVertical')).not.toBeNull();
  });

  it('renders all colors', () => {
    act(() => {
      render(<GcLogo color="black" />, container);
    });
    expect(container.querySelector('svg g path[fill="#000000"]')).not.toBeNull();

    act(() => {
      render(<GcLogo color="blue" />, container);
    });
    expect(container.querySelector('svg g path[fill="#002c77"]')).not.toBeNull();

    act(() => {
      render(<GcLogo color="white" />, container);
    });
    expect(container.querySelector('svg g path[fill="#ffffff"]')).not.toBeNull();
  });
});
