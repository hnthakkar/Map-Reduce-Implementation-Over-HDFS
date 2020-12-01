#!/bin/bash
if [ -d "./log" ]; then   
   rm -rd ./log
fi
if [ -d "./storage" ]; then   
   rm -rd ./storage
fi
if [ ! -d "./bin" ]; then   
   rm -rd ./bin
fi
find . -name "*~" -type f -delete

