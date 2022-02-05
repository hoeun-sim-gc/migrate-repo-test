import React from 'react';
import { Meta, Story } from '@storybook/react';
import { theme } from './../../../src/theme';
import {
  AppBar,
  Badge,
  Divider,
  fade,
  IconButton,
  InputBase,
  makeStyles,
  Theme,
  ThemeProvider,
  Toolbar,
  Typography,
} from '@material-ui/core';
import MenuIcon from '@material-ui/icons/Menu';
import SearchIcon from '@material-ui/icons/Search';
import MailIcon from '@material-ui/icons/Mail';
import MoreVertIcon from '@material-ui/icons/MoreVert';
import NotificationsIcon from '@material-ui/icons/Notifications';
import { AccountCircle } from '@material-ui/icons';
import { createStyles } from '@material-ui/core/styles';
import { GcLogo } from './../gc-logo';

const meta: Meta = {
  title: 'Components/App Bars/Top App Bar (Toolbar)',
  argTypes: {
    color: {
      defaultValue: 'inherit',
      description:
        'The color of the component. It supports those theme colors that make sense for this component.',
      control: {
        type: 'select',
        options: ['default', 'inherit', 'primary', 'secondary', 'transparent'],
      },
    },
    disableGutters: {
      defaultValue: false,
      description: 'If true, disables gutter padding.',
      control: {
        type: 'boolean',
        options: [true, false],
      },
    },
    heading: {
      defaultValue: 'GCUI',
      description: 'Top App Bar heading',
      control: {
        type: 'text',
      },
    },
    position: {
      defaultValue: 'fixed',
      description:
        'The positioning type. The behavior of the different options is described in the MDN web docs. Note: sticky is not universally supported and will fall back to static when unavailable.',
      control: {
        type: 'select',
        options: ['absolute', 'fixed', 'relative', 'static', 'sticky'],
      },
    },
    showHeading: {
      defaultValue: true,
      description: 'If true, the heading will be visible on the top App Bar.',
      control: {
        type: 'boolean',
        options: ['true', 'false'],
      },
    },
    showIconButtons: {
      defaultValue: true,
      description: 'If true, the Icon buttons on top right will be visible on the top App Bar.',
      control: {
        type: 'boolean',
        options: ['true', 'false'],
      },
    },
    showLogo: {
      defaultValue: true,
      description: 'If true, the GCLogo will be visible on the top App Bar.',
      control: {
        type: 'boolean',
        options: ['true', 'false'],
      },
    },
    showNavMenu: {
      defaultValue: true,
      description: 'If true, the navingation menu will be visible on the top App Bar.',
      control: {
        type: 'boolean',
        options: ['true', 'false'],
      },
    },
    showSearch: {
      defaultValue: true,
      description: 'If true, the search field will be visible on the top App Bar.',
      control: {
        type: 'boolean',
        options: ['true', 'false'],
      },
    },
    variant: {
      defaultValue: 'regular',
      description: 'The variant to use.',
      control: {
        type: 'select',
        options: ['regular', 'dense'],
      },
    },
  },
  parameters: {
    controls: { expanded: true },
  },
};
export default meta;

const Template: Story = ({
  heading,
  showSearch,
  showLogo,
  showNavMenu,
  showHeading,
  showIconButtons,
  disableGutters,
  variant,
  ...args
}) => {
  const useStyles = makeStyles((theme: Theme) =>
    createStyles({
      grow: {
        flexGrow: 1,
      },
      logo: {
        width: '150px',
        height: '15px',
        [theme.breakpoints.up('sm')]: {
          width: '200px',
          height: '22px',
        },
      },
      topelements: {
        display: 'flex',
        alignItems: 'center',
      },
      menuButton: {
        marginRight: theme.spacing(2),
      },
      divider: {
        display: 'none',
        [theme.breakpoints.up('sm')]: {
          display: 'block',
        },
      },
      title: {
        display: 'none',
        [theme.breakpoints.up('sm')]: {
          display: 'block',
        },
      },
      search: {
        display: 'none',
        [theme.breakpoints.up('md')]: {
          display: 'flex',
          position: 'relative',
          borderRadius: theme.shape.borderRadius,
          backgroundColor: fade(theme.palette.common.white, 0.15),
          '&:hover': {
            backgroundColor: fade(theme.palette.common.white, 0.25),
          },
          marginRight: theme.spacing(2),
          marginLeft: 0,
          width: '100%',
          justifyContent: 'flex-end',
        },
      },
      searchIcon: {
        padding: theme.spacing(0, 2),
        height: '100%',
        position: 'absolute',
        pointerEvents: 'none',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
      },
      inputRoot: {
        color: 'inherit',
      },
      inputInput: {
        padding: theme.spacing(1, 1, 1, 0),
        // vertical padding + font size from searchIcon
        paddingLeft: `calc(1em + ${theme.spacing(4)}px)`,
        transition: theme.transitions.create('width'),
        width: '100%',
        [theme.breakpoints.up('md')]: {
          width: '20ch',
        },
      },
      sectionDesktop: {
        display: 'none',
        [theme.breakpoints.up('md')]: {
          display: 'flex',
        },
      },
      sectionMobile: {
        display: 'flex',
        [theme.breakpoints.up('md')]: {
          display: 'none',
        },
      },
    })
  );
  const classes = useStyles();

  return (
    <ThemeProvider theme={theme}>
      <div className={classes.grow}>
        <AppBar position="static" {...args}>
          <Toolbar disableGutters={disableGutters} variant={variant}>
            {showNavMenu && (
              <IconButton
                edge="start"
                className={classes.menuButton}
                color="inherit"
                aria-label="open drawer"
              >
                <MenuIcon />
              </IconButton>
            )}
            <span className={classes.topelements}>
              {showLogo && (
                <div className={classes.logo}>
                  <GcLogo color={'blue'} orient={'horizontal'} />
                </div>
              )}
              {heading !== '' && showHeading && (
                <Divider
                  className={classes.divider}
                  orientation="vertical"
                  flexItem
                  variant={'middle'}
                />
              )}
              {showHeading && (
                <Typography className={classes.title} variant="h6" noWrap>
                  {heading}
                </Typography>
              )}
            </span>
            {showSearch && (
              <div className={classes.search}>
                <div className={classes.searchIcon}>
                  <SearchIcon />
                </div>
                <InputBase
                  placeholder="Search…"
                  classes={{
                    root: classes.inputRoot,
                    input: classes.inputInput,
                  }}
                  inputProps={{ 'aria-label': 'search' }}
                />
              </div>
            )}
            <div className={classes.grow} />
            {showIconButtons && (
              <div className={classes.sectionDesktop}>
                <IconButton aria-label="show 4 new mails" color="inherit">
                  <Badge badgeContent={4} color="secondary">
                    <MailIcon />
                  </Badge>
                </IconButton>
                <IconButton aria-label="show 17 new notifications" color="inherit">
                  <Badge badgeContent={17} color="secondary">
                    <NotificationsIcon />
                  </Badge>
                </IconButton>
                <IconButton
                  edge="end"
                  aria-label="account of current user"
                  aria-haspopup="true"
                  color="inherit"
                >
                  <AccountCircle />
                </IconButton>
              </div>
            )}
            {showIconButtons && (
              <div className={classes.sectionMobile}>
                <IconButton aria-label="show more" aria-haspopup="true" color="inherit">
                  <MoreVertIcon />
                </IconButton>
              </div>
            )}
          </Toolbar>
        </AppBar>
      </div>
    </ThemeProvider>
  );
};

export const Playground = Template.bind({});
Playground.args = {};
