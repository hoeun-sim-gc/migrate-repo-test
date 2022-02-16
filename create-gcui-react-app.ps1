param(
    [Parameter(Mandatory=$false)] [string] $appDir = "",
    [Parameter(Mandatory=$false)] [bool] $downloadGCUI = $true,
    [Parameter(Mandatory=$false)] [bool] $useDefaults = $true
)

##################################################
# Create a new GCUI/React app
##################################################

# Constants
$TOTALSTEPS=14
$GCUI_REACT_FILE_PATTERN="react*.tgz"
$GCUI_REACT_URL="https://guycarp.visualstudio.com/GC%20Design%20System/_packaging?_a=feed&feed=gcui%40Local"
$MAX_DOWNLOAD_WAIT=150  # default 150
$SLEEP_BETWEEN_DOWNLOAD_RETRIES=2

# Variables
$global:step=0

function Verify-Prerequisites () {
    Write-Verbose-Message "Verify Prerequisites"
    # npm
    try {
        (npm --version) | Out-Null
    } catch {
        write-host "npm must be installed before proceeding." -ForegroundColor Red
        throw "Missing npm prerequisite."
    }
    # npx is part of npm since 5.2.0 so we do not need to check for it

    # vsts
    try {
        (vsts-npm-auth -?) | Out-Null
    }
    catch {
        write-warning "vsts-npm-auth not installed and is required."
        $answer=
        $answer = read-host "Install vsts-npm-auth? (default: n)"
        if (!($answer.length -gt 0 -and $answer.ToLower() -eq 'y')) {
            Write-Sub-Verbose-Message "Answer was '$answer' so ending script."
            Write-Host "vsts-npm-auth must be installed before proceeding." -ForegroundColor Red
            throw "Missing vsts-npm-auth prerequisite."
        } else {
            Write-Sub-Verbose-Message "Install prerequisite vsts-npm-auth."
            Install-Vsts-Npm-Auth
        }
    }
}

function Write-About-Header () {
    write-host "################################"
    write-host (Get-Date)
    write-host "Creating a new GCUI/React app will take approximately 5 minutes."
    write-host "This script implements the Getting Started Guide for GCUI/React and creates a React application with one GCUI button."
    Write-Host "See the Storybook for GCUI React: http://designsystem.guycarp.com/gcui-react/index.html?path=/story/components-app-bars-top-app-bar-toolbar--playground"
    write-host "################################"

    Write-Verbose-Message "Application Directory is '$($appDir)'"
    Write-Verbose-Message "Download the GCUI tgz instead of nuget is set to $($downloadGCUI)"
    Write-Verbose-Message "Use Defaults for this script is set to $($useDefaults)"
}

function Write-Step-Header ([string]$message) {
    $global:step++
    write-host 
    write-host "################################"
    # write-host (Get-Date)
    write-host "$step of $TOTALSTEPS`: $message  ($(Get-Date))" 
}

function Write-Sub-Step-Header ([int]$subStep, [string]$message) {
    write-host 
    write-host "   ################################"
    write-host "   $step.$subStep of $TOTALSTEPS`: $message"
}

function Write-Verbose-Message ([string]$message) {
    write-verbose "### VERBOSE: $($message) ($(Get-Date))"
}

function Write-Sub-Verbose-Message ([string]$message) {
    write-verbose "   ### VERBOSE: $($message) ($(Get-Date))"
}

function Update-File ([string]$filename, [string]$pattern, [string]$textToAdd, [int]$lineOffset) {
    Write-Verbose-Message "Start function $($MyInvocation.MyCommand)"
    try {
        # find the line with '$pattern'
        $lineNumber = (Select-String -Path $filename -Pattern $pattern | Select-Object * -First 1).LineNumber
        if ($lineNumber -le 0) {
            Write-Error "Could not find '$pattern' in $filename. Exit."
            throw "Could not find '$pattern' in $filename."
        }

        # read all lines into `$fileContents`
        $fileContents = Get-Content $filename

        # append text to desired line
        Write-Verbose-Message "Adding $($textToAdd)"
        $fileContents[$lineNumber+$lineOffset] += $textToAdd

        # write all lines back to file
        $fileContents -join "`n" | Set-Content -Encoding Default -NoNewline $filename
    }
    catch {
        throw $_
    }
    Write-Verbose-Message "End function $($MyInvocation.MyCommand)"
}

function Write-Local-npmrc-File () {
    Write-Verbose-Message "Start function $($MyInvocation.MyCommand)"
    $text = "@gcui:registry=https://guycarp.pkgs.visualstudio.com/_packaging/gcui@Local/npm/registry/"
    $text += "`nalways-auth=true"

    $text | Set-Content ".npmrc"
    Write-Verbose-Message "End function $($MyInvocation.MyCommand)"
}

function Install-Vsts-Npm-Auth () {
    Write-Verbose-Message "Start function $($MyInvocation.MyCommand)"

    Write-Verbose-Message "Start npm install -g vsts-npm-auth"
    npm install -g vsts-npm-auth
    Write-Verbose-Message "End npm install -g vsts-npm-auth"

    Write-Verbose-Message "End function $($MyInvocation.MyCommand)"
}

function Update-User-npmrc-File ([string]$filename, [string]$pattern) {
    Write-Verbose-Message "Start function $($MyInvocation.MyCommand)"
    $filename = "$($env:USERPROFILE)/.npmrc"
    $pattern = "gcui@Local"

    # if the file does not exist or GCUI React isn't defined then catch the exception and create the file
    try {
        if (!(Test-Path $filename)) {
            throw "$($filename) not found"
        }

        if ((Select-String -Path $filename -Pattern $pattern | Select-Object * -First 1).LineNumber -le 0) {
            throw "$($pattern) not found in $($filename)"
        }
    }
    catch {
        Write-Verbose-Message "Start vsts-npm-auth -config .npmrc -TargetConfig $($env:USERPROFILE)/.npmrc"
        vsts-npm-auth -config .npmrc -TargetConfig $env:USERPROFILE/.npmrc
        Write-Verbose-Message "End vsts-npm-auth -config .npmrc -TargetConfig $($env:USERPROFILE)/.npmrc"
    }
    Write-Verbose-Message "End function $($MyInvocation.MyCommand)"
}

function Update-Json-File-Dependencies ([string]$filename, [string]$name, [string]$value) {
    Write-Verbose-Message "Start function $($MyInvocation.MyCommand)"
    try {
        $json = Get-Content $filename -Raw | Out-String | ConvertFrom-Json
        $json.dependencies | Add-Member -Type NoteProperty -Name $name -Value $value
        $json | ConvertTo-Json | Set-Content -Encoding Default -NoNewline $filename
    }
    catch {
        throw $_
    }
    Write-Verbose-Message "End function $($MyInvocation.MyCommand)"
}

function Update-Json-File-Scripts ([string]$filename, [string]$name, [string]$value) {
    Write-Verbose-Message "Start function $($MyInvocation.MyCommand)"

    npm set-script $name $value
    
    # try {
    #     $json = Get-Content $filename -Raw | Out-String | ConvertFrom-Json
    #     $json.scripts | Add-Member -Type NoteProperty -Name $name -Value $value
    #     $json | ConvertTo-Json | Set-Content -Encoding Default -NoNewline $filename
    # }
    # catch {
    #     throw $_
    # }
    Write-Verbose-Message "End function $($MyInvocation.MyCommand)"
}

function Find-GCUI-File () {
    Write-Verbose-Message "Start function $($MyInvocation.MyCommand)"
    $alreadyTimedOutOnce=$false

    try {
        $downloadDir=(join-path $env:USERPROFILE "Downloads")
        Write-Verbose-Message "Searching for $($GCUI_REACT_FILE_PATTERN) in '$($downloadDir)'"

        if (!(Test-Path (join-path $downloadDir $GCUI_REACT_FILE_PATTERN))) {
            Write-Sub-Step-Header 1 "Download gcui/react tgz file from the following link: $($GCUI_REACT_URL)"
            Write-Sub-Verbose-Message "Open $($GCUI_REACT_URL) in browser."
            Start-Process "$($GCUI_REACT_URL)"
            write-host "   The script will continue after the GCUI React package has been downloaded to $($downloadDir)."
            # read-host "Press Any Key after saving the GCUI package to the same folder level of the package.json"
            while (!(Test-Path (join-path $downloadDir $GCUI_REACT_FILE_PATTERN))) {
                # wait
                Start-Sleep $SLEEP_BETWEEN_DOWNLOAD_RETRIES
                $i++
                Write-Sub-Verbose-Message "Still waiting $($i * $SLEEP_BETWEEN_DOWNLOAD_RETRIES) seconds (max wait is $($MAX_DOWNLOAD_WAIT * $SLEEP_BETWEEN_DOWNLOAD_RETRIES))"
                if ($i -gt ($MAX_DOWNLOAD_WAIT * $SLEEP_BETWEEN_DOWNLOAD_RETRIES)) {
                    Write-Sub-Verbose-Message "Timed out waiting after $($i * $SLEEP_BETWEEN_DOWNLOAD_RETRIES) seconds"
                    $i = 1
                    if (! $alreadyTimedOutOnce) {
                        write-host "   Script timed out waiting." -ForegroundColor Yellow
                        read-host "   Press Any Key after saving the GCUI package to $($downloadDir)."
                        $alreadyTimedOutOnce=$true
                    } else {
                        write-debug "   Script timed out again."
                        $answer=
                        $answer = read-host "   Continue waiting for the GCUI package to be downloaded to $($downloadDir)? (default: n)"
                        if (!($answer.length -gt 0 -and $answer.ToLower() -eq 'y')) {
                            Write-Sub-Verbose-Message "Answer was '$answer' so ending script."
                            throw "Do not continue to wait for GCUI package to be downloaded to $($downloadDir)."
                        }
                    }
                }
            }
        } else {
            Write-Verbose-Message "GCUI package already exists in $($downloadDir)."
        }

        if (!(Test-Path (join-path $downloadDir $GCUI_REACT_FILE_PATTERN))) {
            Write-Verbose-Message "GCUI package NOT found in $($downloadDir)."
            Write-Error "GCUI package NOT found in $($downloadDir)."
            throw "GCUI package NOT found in $($downloadDir)."
        }

        $file=((Get-ChildItem -Path $downloadDir -Filter $GCUI_REACT_FILE_PATTERN) | sort Name -desc)[0]
        Write-Verbose-Message "Found GCUI library '$($file.Name)' in $($downloadDir)"

        Write-Verbose-Message "End function $($MyInvocation.MyCommand)"
        return $file
    }
    catch {
        throw $_
    }
    Write-Verbose-Message "End function $($MyInvocation.MyCommand)"
}

function Find-Next-Test-Folder () {
    Write-Verbose-Message "Start function $($MyInvocation.MyCommand)"
    $result="gcui-poc"
    $max=0
    try {
        $folders=Get-ChildItem -Path $pwd -Directory -Filter "gcui-poc*"
        foreach ($folder in $folders) {
            $s = $folder.Name.Replace("gcui-poc", "")
            if ($s.length -gt 0) {
                if ([int]$s -gt $max) {
                    $max = [int]$s
                }
            }
        }

        if ($max -gt 0) {
            $max++
            $result += $max
        } elseif ((Test-Path "gcui-poc")) {
            $result += "1"
        }
    }
    catch {
        throw $_
    }

    Write-Verbose-Message "End function $($MyInvocation.MyCommand)"
    return $result
}

function Create-Simple-GCUI-App () {
    Write-Verbose-Message "Start function $($MyInvocation.MyCommand)"
    try {
        ################################
        Write-Sub-Step-Header 1 "Backup App.js and index.html before modifying."
        Copy-Item ./src/App.js ./src/App.js.original
        Copy-Item ./public/index.html ./public/index.html.original

        ################################
        Write-Sub-Step-Header 2 "Import the GCUI theme and use ThemeProvider component from @material-ui/core/styles and wrap the application."
        $textToAdd = "`nimport { theme } from `"@gcui/react`";"
        $textToAdd += "`nimport { ThemeProvider } from '@material-ui/core';"
        Update-File "./src/App.js" "function" $textToAdd -3

        ################################
        Write-Sub-Step-Header 3 "Add ThemeProvider."
        $textToAdd = "`n< ThemeProvider theme = { theme } >"
        Update-File "./src/App.js" "<div" $textToAdd -2

        $textToAdd = "`n</ThemeProvider>"
        Update-File "./src/App.js" "</div" $textToAdd -1

        ################################
        Write-Sub-Step-Header 4 "Add Button component."
        $textToAdd = "`nimport Button from '@material-ui/core/Button';"
        Update-File "./src/App.js" "function" $textToAdd -3

        $textToAdd = "`n        <p><Button variant = `"contained`" color = `"secondary`" >$($appDir)</Button></p>"
        Update-File "./src/App.js" "<a" $textToAdd -2

        ################################
        Write-Sub-Step-Header 5 "Add following lines for public/index.html at the root level for @material-ui/icons"
        $textToAdd = "`n    <link rel=`"preconnect`" href=`"https://fonts.googleapis.com`">"
        $textToAdd += "`n    <link href=`"https://fonts.googleapis.com/icon?family=Material+Icons`" rel=`"stylesheet`" />"
        Update-File "./public/index.html" "<title>" $textToAdd -2

        ################################
        Write-Sub-Step-Header 6 "Backup copies of App.js and index.html before modifying."
        Copy-Item ./src/App.js ./src/App.js.simple
        Copy-Item ./public/index.html ./public/index.html.simple
    }
    catch {
        throw $_
    }
    Write-Verbose-Message "End function $($MyInvocation.MyCommand)"
}

function Create-Advanced-GCUI-App () {
    Write-Verbose-Message "Start function $($MyInvocation.MyCommand)"
    try {
        ################################
        Write-Sub-Step-Header 1 "Restore backup copies of App.js and index.html before modifying."
        Copy-Item ./src/App.js.original ./src/App.js
        Copy-Item ./public/index.html.original ./public/index.html
        Copy-Item ./src/App.js ./src/App.js.original
        Copy-Item ./public/index.html ./public/index.html.original

        ################################
        Write-Sub-Step-Header 2 "Import the GCUI theme and use ThemeProvider component from @material-ui/core/styles and wrap the application."
        $textToAdd = "`nimport { theme } from `"@gcui/react`";"
        $textToAdd += "`nimport { ThemeProvider } from '@material-ui/core';"
        $textToAdd += "`nimport { makeStyles } from '@material-ui/core/styles';"
        $textToAdd += "`nimport AppBar from '@material-ui/core/AppBar';"
        $textToAdd += "`nimport Toolbar from '@material-ui/core/Toolbar';"
        $textToAdd += "`nimport Typography from '@material-ui/core/Typography';"
        $textToAdd += "`nimport Button from '@material-ui/core/Button';"
        $textToAdd += "`nimport IconButton from '@material-ui/core/IconButton';"
        $textToAdd += "`nimport MenuIcon from '@material-ui/icons/Menu';"
        $textToAdd += "`n"
        $textToAdd += "`nconst useStyles = makeStyles((theme) => ({"
        $textToAdd += "`n  root: {"
        $textToAdd += "`n    flexGrow: 1,"
        $textToAdd += "`n  },"
        $textToAdd += "`n  menuButton: {"
        $textToAdd += "`n    marginRight: theme.spacing(2),"
        $textToAdd += "`n  },"
        $textToAdd += "`n  title: {"
        $textToAdd += "`n    flexGrow: 1,"
        $textToAdd += "`n  },"
        $textToAdd += "`n}));"
        Update-File "./src/App.js" "function" $textToAdd -3

        ################################
        Write-Sub-Step-Header 3 "Add useStyles."
        $textToAdd = "`n  const classes = useStyles();"
        $textToAdd += "`n"
        Update-File "./src/App.js" "function App()" $textToAdd -1

        ################################
        Write-Sub-Step-Header 4 "Add ThemeProvider."
        $textToAdd = "`n    <ThemeProvider theme = { theme }>"
        Update-File "./src/App.js" "<div" $textToAdd -2

        $textToAdd = "`n    </ThemeProvider>"
        Update-File "./src/App.js" "</div" $textToAdd -1

        ################################
        Write-Sub-Step-Header 5 "Add App Layout."
        $textToAdd = "`n    <div className = { classes.root }>"
        $textToAdd += "`n      <AppBar position = `"static`">"
        $textToAdd += "`n      <Toolbar>"
        $textToAdd += "`n      <IconButton edge = `"start`" className = { classes.menuButton } color = `"inherit`">"
        $textToAdd += "`n      <MenuIcon />"
        $textToAdd += "`n      </IconButton>"
        $textToAdd += "`n      <Typography variant = `"h6`""
        $textToAdd += "`n        className = { classes.title }"
        $textToAdd += "`n        color = `"inherit`" >"
        $textToAdd += "`n        $($appDir)"
        $textToAdd += "`n        </Typography>"
        $textToAdd += "`n        <Button color = `"inherit`" > Login < /Button>"
        $textToAdd += "`n      </Toolbar>"
        $textToAdd += "`n      </AppBar>"
        $textToAdd += "`n      </div>"
        Update-File "./src/App.js" "<ThemeProvider theme" $textToAdd -1

        ################################
        Write-Sub-Step-Header 6 "Add Button component."
        $textToAdd = "`n        <p><Button variant = `"contained`" color = `"secondary`" >$($appDir)</Button></p>"
        Update-File "./src/App.js" "App-link" $textToAdd -3

        ################################
        Write-Sub-Step-Header 7 "Remove header tag."
        $filename="./src/App.js"
        (Get-Content $filename) -notmatch "<header className" -join "`n" | out-file -Encoding Default -NoNewline $filename
        (Get-Content $filename) -notmatch "</header>" -join "`n" | out-file -Encoding Default -NoNewline $filename

        ################################
        Write-Sub-Step-Header 6 "Add following lines for public/index.html at the root level for @material-ui/icons"
        $textToAdd = "`n    <link rel=`"preconnect`" href=`"https://fonts.googleapis.com`">"
        $textToAdd += "`n    <link href=`"https://fonts.googleapis.com/icon?family=Material+Icons`" rel=`"stylesheet`" />"
        Update-File "./public/index.html" "<title>" $textToAdd -2

        ################################
        Write-Sub-Step-Header 7 "Backup copies of App.js and index.html before modifying."
        Copy-Item ./src/App.js ./src/App.js.advanced
        Copy-Item ./public/index.html ./public/index.html.advanced
    }
    catch {
        throw $_
    }
    Write-Verbose-Message "End function $($MyInvocation.MyCommand)"
}

################################################################
# Main
################################################################
cls

try {
    Write-About-Header

    Verify-Prerequisites

    ################################
    Write-Step-Header "Get project directory"
    if ($appDir.Length -le 0) {
        $appDir = Find-Next-Test-Folder
    }

    if ($useDefaults -eq $false) {
        $answer=
        $answer = read-host "Enter the project directory (default: $($appDir))"
        if ($answer.length -gt 0) {
            $appDir = $answer
        }

        $answer=
        $answer = read-host "Download and use local GCUI React library? (default: n)"
        if ($answer.length -gt 0 -and $answer.ToLower() -eq 'y') {
            $downloadGCUI = $true
        } else {
            $downloadGCUI = $false
        }
    } else {
        write-host "Project directory is " -NoNewline; write-host -ForegroundColor White "$($appDir)"
    }
    Write-Verbose-Message "Application Directory is '$($appDir)'"
    Write-Verbose-Message "Download the GCUI tgz instead of nuget is set to $($downloadGCUI)"


    if ($appDir.Length -lt 1) {
        throw "Project directory is required."
    }

    ################################
    if ($downloadGCUI -eq $true) {
        Write-Step-Header "Use local gcui/react tgz file"
        $gcuiReactFile = Find-GCUI-File
        write-host "Found GCUI library: $($gcuiReactFile.Name)"
    }

    ################################
    Write-Step-Header "Create the react workspace"
    Write-Verbose-Message "Start npx create-react-app $($appDir)"
    npx create-react-app $appDir
    Write-Verbose-Message "End npx create-react-app"
    Push-Location $appDir

    ################################
    Write-Step-Header "Install GCUI dependencies"
    Write-Verbose-Message "Start npm install --save @material-ui/core @material-ui/icons"
    npm install --save @material-ui/core @material-ui/icons
    Write-Verbose-Message "End npm install"

    ################################
    Write-Step-Header "Install optional GCUI dependencies"
    Write-Verbose-Message "Start npm install --save @material-ui/data-grid @material-ui/lab"
    npm install --save @material-ui/data-grid @material-ui/lab
    Write-Verbose-Message "End npm install"
    
    ################################
    Write-Verbose-Message "Save package.json as package.json.original"
    Copy-Item ./package.json ./package.json.original

    ################################
    Write-Step-Header "Install GCUI"
    # Update-Json-File "./package.json" "@gcui/react" "file:$($gcuiReactFile.Name)"
    # $textToAdd = "`n  `"@gcui/react`":  `"file:react-0.3.1.tgz`","
    # Update-File "package.json" "dependencies" $textToAdd 0
    if ($downloadGCUI -eq $true) {
        Write-Verbose-Message "Place gcui/react in the same folder level of the package.json"
        Write-Verbose-Message 'Copy $((join-path (join-path $env:USERPROFILE "Downloads")) $gcuiReactFile.Name) to$((join-path $pwd $gcuiReactFile.Name))'
        Copy-Item (join-path (join-path $env:USERPROFILE "Downloads") $gcuiReactFile.Name) (join-path $pwd $gcuiReactFile.Name)

        Write-Verbose-Message "Start npm install $($gcuiReactFile.Name)"
        npm install $gcuiReactFile.Name
        Write-Verbose-Message "End npm install"
    } else {
        Write-Local-npmrc-File

        Update-User-npmrc-File

        # Update-Json-File-Scripts "./package.json" "refreshVSToken" "vsts-npm-auth -config .npmrc"
        Write-Verbose-Message "Start npm set-script refreshVSToken vsts-npm-auth -config .npmrc"
        npm set-script "refreshVSToken" "vsts-npm-auth -config .npmrc"
        Write-Verbose-Message "End npm set-script refreshVSToken vsts-npm-auth -config .npmrc"

        Write-Verbose-Message "Start npm run refreshVSToken"
        npm run refreshVSToken
        Write-Verbose-Message "End npm run refreshVSToken"

        Write-Verbose-Message "Start npm install @gcui/react"
        npm install --save @gcui/react
        Write-Verbose-Message "End npm install @gcui/react"
    }

    ################################
    Write-Step-Header "Install the node modules."
    Write-Verbose-Message "Start npm install"
    npm install
    Write-Verbose-Message "End npm install"

    ################################
    Write-Step-Header "Create simple gcui app"
    Create-Simple-GCUI-App

    ################################
    Write-Step-Header "Create advanced gcui app"
    Create-Advanced-GCUI-App
    
    ################################
    Write-Verbose-Message "Return to original directory before starting $($appDir)"
    # This is so the script is ready to run again. 
    Pop-Location

    ################################
    Get-Date

    ################################
    Write-Step-Header "Npm start --prefix $appDir"
    Write-Verbose-Message "Start npm start --prefix $($appDir)"
    npm start --prefix $appDir
    Write-Verbose-Message "End npm start"
}
catch {
    Write-Error $_
    Write-Host -ForegroundColor Red "Exit."
}
