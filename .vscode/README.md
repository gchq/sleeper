VSCode Setup
=======

## Introduction
Java development is not supported out of the box and several plugins needs to be installed and configured. Once this setup is complete VSCode offers a good development environment. It will enable intellisense, spell checking, code formatting and checksyles. The versions of plugins defined were the most up to date and tested versions at the time of writing.

This guide assumes that VSCode is already installed.

### Extension
The file `extensions.json` in this repository shows which extensions have been used in this setup. They will need installing via the Extension tab. 

The extensions will be installed at `~/.vscode-server/extensions/` along with a copy of `extensions.json`

### Settings
There are a number of locations where `settings.json` can live but for this setup it is assumed they will be applied at the workspace level. 

To update the settings in VSCode navigate to `Code - Settings ... - Settings - Workspace`. To the top right of the tab is a file symbol with a hover text `Open Settings (JSON)`.  Clicking this button will open a JSON editor and merge the the `settings.json` file from this repository with the current one.

### Issues
Due to there being two `settings.json` files (three if using Remote Containers) it is possible that settings are being overridden by another one. The order they are loaded are `User - Remote - Workspace`.

