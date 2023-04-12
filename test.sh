#!/bin/bash

#cd /home/dr-saeed/Desktop/upwork/Garbage # Go to HW directory
rm -rf build # Remove 'build' folder with all its contents
mkdir build # Create 'build' folder
cd build # Go there
cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON .. # CMake (buils project)
cmake --build . # CMake (buils project) 2
./container-explorer-tests # Run tests
