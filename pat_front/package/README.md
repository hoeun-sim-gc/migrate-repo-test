# GCUI React

# Getting Started

## Links

* [Download](https://guycarp.visualstudio.com/GC%20Design%20System/_packaging?_a=package&feed=gcui%40Local&package=%40gcui%2Freact&protocolType=Npm)

## Prerequisites

* [Node](https://nodejs.org/en/)


## Setup GCUI in a New React Workspace 

### 1. Create an React Workspace

Create react app: `npx create-react-app my-app`

next: `npx create-next-app my-app`

### 2. Install GCUI's Dependencies

`npm install --save @material-ui/core @material-ui/icons`

### 3. Install the optional dependencies if needed

`npm install --save @material-ui/data-grid @material-ui/lab`

### 4. `.npmrc` setup

The recommended way to install GCUI is by configuring your project and user `.npmrc` files. Check `How to Install GCUI via .npmrc` instruction below. Once you're setup it'll be as easy as installing any other module.

### 5. Install GCUI-React Module 

`npm install --save @gcui/react`

### 6. Install the Modules

`npm install`

### 7. Import GCUI's theme and use ThemeProvider component for gcui theme

``` html
import { theme } from "@gcui/react";

<ThemeProvider theme={theme}>
  ...
</ThemeProvider>
```

### 8. Link to Material Icons

#### index.html

``` html
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet" />
```

### 9. Run Your App

`npm start`

---

# How to Install GCUI via `.npmrc`

## Connect to the Design Feed

Begin by adding an `.npmrc` file to your project in the same directory as your `package.json`. *Be sure to include the channel in your url.* This example uses the @Release channel, encoded as `%40Release`.

```
@gcui:registry=https://guycarp.pkgs.visualstudio.com/_packaging/gcui/npm/registry/
always-auth=true
```

Next, you'll need to configure your user-level `.npmrc` file. For more details check out [the documentation](https://docs.microsoft.com/en-us/azure/devops/artifacts/npm/npmrc?view=azure-devops&tabs=windows) for Windows and Mac/Linux.

### Windows `.npmrc`

Run `vsts-npm-auth` to get an Azure Artifacts token added to your user-level `.npmrc` file

```
vsts-npm-auth -config .npmrc
```

### Mac/Linux `.npmrc`

#### Step 1

Copy the code below to your user `.npmrc`. The URL needs to match the one in your project's `.npmrc` file. *Be sure to include the channel you want to connect to.*

```
; begin auth token
//guycarp.pkgs.visualstudio.com/_packaging/gcui/npm/registry/:username=[Your.Name@mmc.com]
//guycarp.pkgs.visualstudio.com/_packaging/gcui/npm/registry/:_password=[BASE64_ENCODED_PERSONAL_ACCESS_TOKEN]
//guycarp.pkgs.visualstudio.com/_packaging/gcui/npm/registry/:email=[Your.Name@mmc.com]
//guycarp.pkgs.visualstudio.com/_packaging/gcui/npm/:username=[Your.Name@mmc.com]
//guycarp.pkgs.visualstudio.com/_packaging/gcui/npm/:_password=[BASE64_ENCODED_PERSONAL_ACCESS_TOKEN]
//guycarp.pkgs.visualstudio.com/_packaging/gcui/npm/:email=[Your.Name@mmc.com]
; end auth token
```

#### Step 2

Generate a Personal Access Token with Packaging read & write scopes.

#### Step 3

Base64 encode the personal access token from Step 2.

```
echo -n "personalAccessToken" | base64
```

#### Step 4

1. Replace both `[BASE64_ENCODED_PERSONAL_ACCESS_TOKEN]` values in your user `.npmrc` file with your personal access token from Step 3.
2. Replace each `[Your.Name@mmc.com]` with your email.

---

## Commands

Built with [TSDX](https://tsdx.io/).

TSDX scaffolds your new library inside `/src`, and also sets up a [Parcel-based](https://parceljs.org) playground for it inside `/example`.

The recommended workflow is to run TSDX in one terminal:

```bash
npm start # or yarn start
```

This builds to `/dist` and runs the project in watch mode so any edits you save inside `src` causes a rebuild to `/dist`.

Then run either Storybook or the example playground:

### Storybook

Run inside another terminal:

```bash
npm run storybook
```

This loads the stories from `./stories`.

> NOTE: Stories should reference the components as if using the library, similar to the example playground. This means importing from the root project directory. This has been aliased in the tsconfig and the storybook webpack config as a helper.

### Example

Then run the example inside another:

```bash
cd example
npm i # or yarn to install dependencies
npm start # or yarn start
```

The default example imports and live reloads whatever is in `/dist`, so if you are seeing an out of date component, make sure TSDX is running in watch mode like we recommend above. **No symlinking required**, we use [Parcel's aliasing](https://parceljs.org/module_resolution.html#aliases).

To do a one-off build, use `npm run build` or `yarn build`.

To run tests, use `npm test` or `yarn test`.

## Configuration

Code quality is set up for you with `prettier`, `husky`, and `lint-staged`. Adjust the respective fields in `package.json` accordingly.

### Jest

Jest tests are set up to run with `npm test` or `yarn test`.

### Bundle analysis

Calculates the real cost of your library using [size-limit](https://github.com/ai/size-limit) with `npm run size` and visulize it with `npm run analyze`.

#### Setup Files

This is the folder structure we set up for you:

```txt
/example
  index.html
  index.tsx       # test your component here in a demo app
  package.json
  tsconfig.json
/src
  index.tsx       # EDIT THIS
/test
  blah.test.tsx   # EDIT THIS
/stories
  Thing.stories.tsx # EDIT THIS
/.storybook
  main.js
  preview.js
.gitignore
package.json
README.md         # EDIT THIS
tsconfig.json
```

#### React Testing Library

We do not set up `react-testing-library` for you yet, we welcome contributions and documentation on this.

### Rollup

TSDX uses [Rollup](https://rollupjs.org) as a bundler and generates multiple rollup configs for various module formats and build settings. See [Optimizations](#optimizations) for details.

### TypeScript

`tsconfig.json` is set up to interpret `dom` and `esnext` types, as well as `react` for `jsx`. Adjust according to your needs.

## Continuous Integration

### GitHub Actions

Two actions are added by default:

- `main` which installs deps w/ cache, lints, tests, and builds on all pushes against a Node and OS matrix
- `size` which comments cost comparison of your library on every pull request using [size-limit](https://github.com/ai/size-limit)

## Optimizations

Please see the main `tsdx` [optimizations docs](https://github.com/palmerhq/tsdx#optimizations). In particular, know that you can take advantage of development-only optimizations:

```js
// ./types/index.d.ts
declare var __DEV__: boolean;

// inside your code...
if (__DEV__) {
  console.log('foo');
}
```

You can also choose to install and use [invariant](https://github.com/palmerhq/tsdx#invariant) and [warning](https://github.com/palmerhq/tsdx#warning) functions.

## Module Formats

CJS, ESModules, and UMD module formats are supported.

The appropriate paths are configured in `package.json` and `dist/index.js` accordingly. Please report if any issues are found.

## Named Exports

Per Palmer Group guidelines, [always use named exports.](https://github.com/palmerhq/typescript#exports) Code split inside your React app instead of your React library.

## Including Styles

There are many ways to ship styles, including with CSS-in-JS. TSDX has no opinion on this, configure how you like.

For vanilla CSS, you can include it at the root directory and add it to the `files` section in your `package.json`, so that it can be imported separately by your users and run through their bundler's loader.
