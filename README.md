# memurai-sql

## **This is a Beta version of Memurai SQL. Please do not use in production!**

Memurai SQL requires Memurai 4.3 or Valkey 7.2.

## Build

### Windows

To build memurai-sql on Windows, just install Visual Studio 2022 and open the folder. It will use the `CMakeLists.txt` file to configure the project and then is just to run `Build All` to create memurai-sql module.

### Ubuntu 24.04

Install `gcc`:
```
sudo apt install -y gcc
```

Now it is possible to build memurai-sql by running:
```
mkdir build
cd build/
cmake ..
make
```

## Load module

### Windows

Load the module using `memurai-cli`:
```
memurai-cli module load <FULLPATH>\memurai-sql.dll
```

### Ubuntu 24.04

It is possible to install Load the module using `valkey-cli`:
```
valkey-cli module load <FULLPATH>/libmemurai-sql.so
```

## Commands

For Memurai SQL commands and configuration please checkout [Memurai SQL Documentation](https://docs.memurai.com/en/msql-overview).

\* Valkey is a trademark of Linux Foundation. Any rights therein are reserved to Linux Foundation. Any use by Memurai is for referential purposes only.

