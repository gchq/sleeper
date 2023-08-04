VSCode Setup
=======

## Introduction
Java development is not supported out of the box and several plugins needs to be installed and configured. Once this setup is complete VSCode offers a good development environment. It will enable intellisense, spell checking, code formatting and checksyles. The versions of plugins defined were the most up to date and tested versions at the time of writing.

This guide assumes that VSCode is already installed.

### Extension
Below are a list of extension and versions that this guide has been tested with. They will need installing via the Extension tab. 

- bierner.markdown-preview-github-styles: 2.0.2
- github.vscode-pull-request-github: 0.66.2
- github.vscode-github-actions: 0.25.8
- mhutchie.git-graph: 1.30.0
- redhat.vscode-yaml: 1.13.0
- shengchen.vscode-checkstyle: 1.4.2
- streetsidesoftware.code-spell-checker: 2.20.5
- visualstudioexptteam.intellicode-api-usage-examples: 0.2.7


These are all installed as part of the VSCode Java Pack
- vscjava.vscode-java-pack: 0.25.12
- redhat.java: 1.20.0
- vscjava.vscode-java-debug: 0.52.0
- vscjava.vscode-java-dependency: 0.23.0
- vscjava.vscode-java-test: 0.39.0
- vscjava.vscode-maven: 0.41.0
- visualstudioexptteam.vscodeintellicode: 1.2.30


The extensions will be installed at `~/.vscode-server/extensions/`

### Settings
There are a number of locations where `settings.json` can live but for this setup it is assumed they will be applied at the workspace level. 

To update the settings in VSCode navigate to `Code - Settings ... - Settings - Workspace`. To the top right of the tab is a file symbol with a hover text `Open Settings (JSON)`.  Clicking this button will open a JSON editor and merge the the `settings.json` file from this repository with the current one.

Note: The following JSON paths will need updating to include the location of your installed Java JDKs. Remove `comment.` from the start of the keys.
- `java.configuration.runtimes.[].path`
- `java.jdt.ls.java.home`

### Issues
Due to there being two `settings.json` files (three if using Remote Containers) it is possible that settings are being overridden by another one. The order they are loaded are `User - Remote - Workspace`.

