#!/bin/bash
if [ ! -d "./log" ]; then   
   mkdir ./log
fi
if [ ! -d "./output" ]; then   
   mkdir ./output
fi
if [ ! -d "./bin" ]; then   
   mkdir ./bin
fi
if [ ! -d "./tmp" ]; then   
   mkdir ./tmp
fi
echo "Compilation all java classes"
javac -cp "./lib/*" -d ./bin $(find ./src/* | grep .java)
echo "Successful"

java -cp "./lib/*:bin/" JobClientDriver


