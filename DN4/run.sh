#!/bin/bash
if [ ! -d "./log" ]; then   
   mkdir ./log
fi
if [ ! -d "./storage" ]; then   
   mkdir ./storage
fi
if [ ! -d "./bin" ]; then   
   mkdir ./bin
fi
echo "Compilation all java classes"
javac -cp "./lib/*" -d ./bin $(find ./src/* | grep .java)
echo "Successful"

java -cp "./lib/*:bin/" DNDriver


