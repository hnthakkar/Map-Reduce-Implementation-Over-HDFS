#!/bin/bash
if [ -d "./log" ]; then   
   rm -rd ./log
fi
if [ -d "./bin" ]; then   
   rm -rd ./bin
fi
if [ -d "./tmp" ]; then   
   rm -rd ./tmp
fi
find . -name "*~" -type f -delete


