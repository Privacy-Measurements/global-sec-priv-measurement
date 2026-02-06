# Building Brave Browser 

This guide describes the steps to build a custom version of the Brave browser used for our project. This modified version enables PageGraph to **track more JavaScript APIs** than the default Brave Nightly. Specifically, it tracks all APIs listed in the **Tracked_APIS.py** file.


These steps are based on instructions from the official [brave-browser GitHub repository](https://github.com/brave/brave-browser), with some details for installing dependecies and integrating of the tracked APIs.



## Prerequisites

We built this on an **Intel x86-64 Ubuntu machine**, and the output browser is also intended for an **Intel x86-64 Linux machine**

Required tools and versions:

- **Python 3.10** (in a virtual environment is recommended)
- **Node.js v20**
- **Linux** system with `apt` package manager (Ubuntu/Debian-based)



## Evironment setup

Install system packages and Python dependencies:

```bash
$ sudo apt-get install build-essential python-setuptools python3-distutils
$ sudo apt-get install build-essential 
$ sudo apt-get install pkg-config 
$ pip install essentials 
$ pip install standard-distutils
```


## Cloning Brave Browser source code and initializing it 


```bash
$ git clone https://github.com/brave/brave-browser.git
$ cd brave-browser
$ npm install
$ npm run init
```


## Installing build dependencies 


```bash
$ ./src/build/install-build-deps.sh
```



## Placing the tracked APIs and creating the .env file

1. Replace the **_PAGE_GRAPH_TRACKED_ITEMS** object in 

```bash
$ src/brave/chromium_src/third_party/blink/renderer/bindings/scripts/bind_gen/interface.py
```

with the version from:

```bash
$ ./Tracked_APIS.py
```


2. Place the **.env** file inside:

```bash
$ src/brave
```


## Building the browser


```bash
$ npm run build -- Static
```


